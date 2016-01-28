package linalg.operations

import breeze.linalg.{DenseMatrix, DenseVector}

/**
  * Created by manpreet.singh on 28/01/16.
  */
object common {

  // vector's
  val v1 = DenseVector(3, 7, 8.1, 4, 5)
  val v2 = DenseVector(1, 9, 3, 2.3, 8)

  // column matrix (3 * 3)
  val m1 = DenseMatrix((2, 3, 1), (4, 5, 1), (6, 7, 1))
  val m2 = DenseMatrix((3, 4, 1), (2, 6, 1), (8, 2, 1))

  // add operation
  def add(): Unit = {
    println(v1 + v2)
    println(m1 + m2)
  }

  // mul operation
  def mul(): Unit = {
    println(v1 :* v2)
    println(m1 :* m2)
  }

  // compare operation
  def compare(): Unit = {
    println(v1 :< v2)
    println(m1 :< m2)
  }

  // inplace addition operation
  def inplace(): Unit = {
    println(v1 :+= 2)
    println(m1 :+= 3)
  }


  def main(args: Array[String]) {
    add()
    mul()
    compare()
    inplace()
  }
}
