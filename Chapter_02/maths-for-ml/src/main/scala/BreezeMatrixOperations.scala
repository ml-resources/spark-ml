import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.linalg.functions._
import breeze.math._
import breeze.numerics._
import breeze.math.Complex
import breeze.util.DoubleImplicits
import breeze.linalg._



object BreezeMatrixOperations {

  def main(args: Array[String]) {
    val a = DenseMatrix((1,2),(3,4))
    val b = DenseMatrix((2,2),(2,2))
    val c = a + b
    println("a: \n" + a)
    println("b: \n" + b)
    println("a + b : \n" + c)
    val d = a*b
    println("Dot product a*b : \n" + d)

    val e = 	a :+= 1
    val f = a :< b

    println("Inplace Addition : a :+= 1\n" + e)

    val g = DenseMatrix((1.1, 1.2), (3.9, 3.5))
    println("g: \n" + g)
    val gCeil =ceil(g)
    println("ceil(g)\n " + gCeil)

    val gFloor =floor(g)
    println("floor(g)\n" + gFloor)

    val sumA = sum(a)
    println("sum(a):\n" + sumA)
    println("a.max:\n" + a.max)
    println("argmax(a):\n" + argmax(a))

  }

}
