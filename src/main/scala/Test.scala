import cats.Functor
import matryoshka.{Algebra, Coalgebra}
import cats.implicits._
sealed trait Tree
case class Node(left: Tree, right: Tree) extends Tree
case class Leaf(label: Int)              extends Tree

sealed trait TreeF[A]
case class NodeF[A](left: A, right: A) extends TreeF[A]
case class LeafF[A](label: Int)        extends TreeF[A]

object TreeF {
  implicit val treeFunctor = new Functor[TreeF] {
    override def map[A, B](fa: TreeF[A])(f: A => B): TreeF[B] = fa match {
      case NodeF(left, right) => NodeF(f(left), f(right))
      case LeafF(label)       => LeafF(label)
    }
  }
}

final case class Fix[F[_]](unfix: F[Fix[F]])

object test {

  def hylo[F[_]: Functor, A, B](a: A)(alg: Algebra[F, B], coalg: Coalgebra[F, A]): B =
    alg(coalg(a) map (hylo(_)(alg, coalg)))

  val tree: Tree                       = Node(Leaf(1), Leaf(2))
  val treeF: TreeF[String]             = NodeF("abc", "def")
  val t: TreeF[LeafF[Nothing]]         = NodeF(LeafF(1), LeafF(0))
  val t2: TreeF[NodeF[LeafF[Nothing]]] = NodeF(NodeF(LeafF(1), LeafF(0)), NodeF(LeafF(1), LeafF(2)))
  val ft: Fix[TreeF] = Fix[TreeF](
    NodeF(
      Fix[TreeF](LeafF(1)),
      Fix[TreeF](LeafF(1))
    )
  )
  val ft2: Fix[TreeF] = Fix[TreeF](
    NodeF(
      Fix[TreeF](
        NodeF(
          Fix[TreeF](LeafF(1)),
          Fix[TreeF](LeafF(0))
        )
      ),
      Fix[TreeF](LeafF(1))
    )
  )

}
