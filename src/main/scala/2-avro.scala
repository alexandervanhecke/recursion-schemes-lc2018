package lc2018

import org.apache.avro.{LogicalTypes, _}
import matryoshka._, implicits._, patterns.EnvT
import scala.collection.immutable.ListMap
import scalaz._, Scalaz._

import scala.language.higherKinds
import scala.collection.JavaConverters._

/**
  * There is a problem that makes writing SchemaF <-> Avro (co)algebras more difficult.
  *
  * As a matter of fact Avro mandates that, when building a Schema, all records (the Avro
  * equivalent to our StructF) are registered using a unique name.
  *
  * This is problematic to our algebra-based method because with the algebras we've seen so
  * far we only care about one "layer" at a time, so there is no way to know the names we've
  * already used for ther records we've registered so far.
  *
  * Fortunately, we have at least two solutions to that problem. But before going any further,
  * maybe you can take a few minutes to try and imagine how we can solve that problem in general,
  * even if you don't know how to implement your solution using recursion-schemes yet.
  */
trait SchemaToAvroAlgebras extends Labelling with UsingARegistry with AvroCoalgebra {}

/**
  * The first solution comes from the observation that our schemas are in fact trees. And trees have
  * this nice property that each node have a unique path that goes from the root to it. If we can use
  * that unique path as the names of our records, we're good to go. So this solution boils down to
  * labelling each "node" of a schema with its path, and then use that path to form the names we
  * use to register our records.
  */
trait Labelling {

  /**
    * So lets define out Path as being simply a list of strings. These strings will be the field names
    * we need to traverse from the root to get to a specific element of our schema.
    */
  type Path = List[String]

  /**
    * Here is the "special trick" of the current solution.
    *
    * EnvT is a kind of "glorified pair". Given a label type E and a (pattern)-functor F, it allows us
    * to label each "node" of a T[F] with a value of type E while retaining the original structure. In
    * other words, if F is a functor, then EnvT[E, F, ?] is a functor as well.
    */
  type Labelled[A] = EnvT[Path, SchemaF, A]

  /**
    * If we are to label each "node" of a schema with its own path, we obviously need to go from the root
    * down to the leaves, so we definitely want to write a coalgebra.
    * This one might look a bit scarry though, but fear not, it's not as complcated as it looks. Lets just
    * follow the types together.
    *
    * A Coalgebra[F, A] is just a function A => F[A]. So the coalgebra bellow is just a function
    *  (Path, T[SchemaF]) => Labelled[(Path, T[SchemaF])
    * Expanding the Labelled alias it becomes
    *  (Path, T[SchemaF]) => EnvT[Path, SchemaF, (Path, T[SchemaF])]
    *
    * Ok, maybe it still looks a bit scarry...
    *
    * Lets try to put it differently. Assume you will be given a "seed" consisting of a whole schema and an
    * initial path (that will start empty). Your job is to use that to produce an EnvT that will contain
    * the path of the node you just saw (the "root" of the schema that was in the seed), and the node itself
    * but modified such that its "content" is not just a "smaller schema" as it was initially, but a new "seed"
    * consisting of a (larger) path, and the said "smaller schema".
    */
  def labelNodesWithPath[T](implicit T: Recursive.Aux[T, SchemaF]): Coalgebra[Labelled, (Path, T)] = {
    case (path, schema) =>
      schema.project match {
        case StructF(fields) =>
          EnvT(path -> StructF(fields.map { case (key, value) => key -> (path :+ key, value) }))
        case other => EnvT(path -> other.map(x => (path, x)))
      }
  }

  /**
    * Now the algebra (that we had no way to write before) becomes trivial. All we have to do is to use
    * the path labelling each "node" as the name we need when registering a new avro record.
    *
    * To extract the label (resp. node) of an EnvT you can use pattern-matching (EnvT contains only a pair
    * (label, node)), or you can use the `ask` and `lower` methods that return the label and node respectively.
    * EnvT[Path, SchemaF, Schema] => Schema
    */
  def labelledToSchema: Algebra[Labelled, Schema] = { envt =>
    val path: Path            = envt.ask
    val node: SchemaF[Schema] = envt.lower
    node match {
      case StructF(fields) =>
        fields
          .foldLeft(SchemaBuilder.record(path.mkString("a", ".", "z")).fields) {
            case (builder, (key, value)) =>
              builder.name(key).`type`(value).noDefault()
          }
          .endRecord()
      case ArrayF(element) => Schema.createArray(element)
      case BooleanF()      => Schema.create(Schema.Type.BOOLEAN)
      case DateF()         => LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))
      case DoubleF()       => Schema.create(Schema.Type.DOUBLE)
      case FloatF()        => Schema.create(Schema.Type.FLOAT)
      case IntegerF()      => Schema.create(Schema.Type.INT)
      case LongF()         => Schema.create(Schema.Type.LONG)
      case StringF()       => Schema.create(Schema.Type.STRING)
    }
  }

  def schemaFToAvro[T](schemaF: T)(implicit T: Recursive.Aux[T, SchemaF]): Schema =
    (List.empty[String], schemaF).hylo(labelledToSchema, labelNodesWithPath)
}

/**
  * That first solution was (relatively) simple but it is not completely satisfying.
  * We needed both an algebra and a coalgebra to got from our SchemaF to Avro's Schema, which forced us to
  * use hylo.
  *
  * Fortunately, every scheme (and the related algebra) come with a "monadic" version. In this version, we
  * have to "wrap" the result of our algebras inside our monad of choice. The scheme will then use this
  * monad's bind at each step. That has plenty of cool uses.
  *
  * We can for example "short-circuit" the traversal by using \/ or Option as our monad. Or in this very case
  * we can use the State monad to keep track of what records we've already created.
  *
  * A note though: in order to use monadic schemes, we need a Traverse instance for our pattern-functor.
  */
trait UsingARegistry {

  type Registry[A] = State[Map[Int, Schema], A]

  def fingerprint(fields: Map[String, Schema]): Int = fields.hashCode

  // SchemaF[Schema] => Registry[Schema]
  def useARegistry: AlgebraM[Registry, SchemaF, Schema] = {
    case StructF(fields) =>
      val fp = fingerprint(fields)
      State { reg: Map[Int, Schema] =>
        if (reg contains fp) {
          (reg, reg(fp))
        } else {
          val record =
            fields
              .foldLeft(SchemaBuilder.record("r%x".format(fp)).fields) {
                case (builder, (k, v)) =>
                  builder.name(k).`type`(v).noDefault
              }
              .endRecord
          (reg + (fp -> record), record)
        }
      }
    case ArrayF(element) => State(s => (s, Schema.createArray(element)))
    case BooleanF()      => State(s => (s, Schema.create(Schema.Type.BOOLEAN)))
    case DateF()         => State(s => (s, LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))))
    case DoubleF()       => State(s => (s, Schema.create(Schema.Type.DOUBLE)))
    case FloatF()        => State(s => (s, Schema.create(Schema.Type.FLOAT)))
    case IntegerF()      => State(s => (s, Schema.create(Schema.Type.INT)))
    case LongF()         => State(s => (s, Schema.create(Schema.Type.LONG)))
    case StringF()       => State(s => (s, Schema.create(Schema.Type.STRING)))
  }

  implicit def schemaFTraverse: Traverse[SchemaF] = new Traverse[SchemaF] {
    override def traverseImpl[G[_], A, B](fa: SchemaF[A])(f: A => G[B])(implicit G: Applicative[G]): G[SchemaF[B]] =
      fa match {
        case StructF(fields) => {
          val (keys, values)     = fields.unzip
          val result: G[List[B]] = G.traverse(values.toList)(f)
          result.map(values => StructF(ListMap(keys.toList zip values: _*)))
        }
        case ArrayF(element) => f(element).map(ArrayF(_))
        case BooleanF()      => G.pure(BooleanF())
        case DateF()         => G.pure(DateF())
        case DoubleF()       => G.pure(DoubleF())
        case FloatF()        => G.pure(FloatF())
        case IntegerF()      => G.pure(IntegerF())
        case LongF()         => G.pure(LongF())
        case StringF()       => G.pure(StringF())
      }
  }

  def toAvro[T](schemaF: T)(implicit T: Recursive.Aux[T, SchemaF]): Schema =
    schemaF.cataM(useARegistry).run(Map.empty)._2
}

trait AvroCoalgebra {

  /**
    * Of course we also need a coalgebra to go from Avro to SchemaF
    * Since there are some avro shcemas that we do not handle here,
    * we need a CoalgebraM, but we're not really interested in providing meaningful errors
    * here, so we can use Option as our monad.
    * Schema => Option[SchemaF[Schema]]
    */
  def avroToSchemaF: CoalgebraM[Option, SchemaF, Schema] = { schema =>
    schema.getType match {
      case Schema.Type.RECORD  => Some(StructF(ListMap(schema.getFields.asScala.map(f => f.name -> f.schema): _*)))
      case Schema.Type.ARRAY   => Some(ArrayF(schema.getElementType))
      case Schema.Type.BOOLEAN => Some(BooleanF())
      case Schema.Type.DOUBLE  => Some(DoubleF())
      case Schema.Type.FLOAT   => Some(FloatF())
      case Schema.Type.INT     => Some(IntegerF())
      case Schema.Type.LONG =>
        val lt = schema.getLogicalType
        if (lt != null) {
          if (lt.getName == LogicalTypes.timestampMillis().getName) {
            DateF().some
          } else None
        } else LongF().some
      case Schema.Type.STRING => Some(StringF())
      case _                  => None
    }

  }
}
