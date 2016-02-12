package linalg.vector
import org.apache.spark.mllib.linalg.{Vector, Vectors}

object SparkVector {

  def main(args: Array[String]): Unit = {
    // Create a dense vector (1.0, 0.0, 3.0).

    val dVectorOne: Vector = Vectors.dense(1.0, 0.0, 2.0)
    println("dVectorOne:" + dVectorOne)
    //  Sparse vector (1.0, 0.0, 2.0, 3.0)
    // corresponding to nonzero entries.
    val sVectorOne: Vector = Vectors.sparse(4,  Array(0, 2,3),  Array(1.0, 2.0, 3.0))
    // Create a sparse vector (1.0, 0.0, 2.0, 2.0) by specifying its
    // nonzero entries.
    val sVectorTwo: Vector = Vectors.sparse(4,  Seq((0, 1.0), (2, 2.0), (3, 3.0)))

    println("sVectorOne:" + sVectorOne)
    println("sVectorTwo:" + sVectorTwo)
    /*
    abstract def
    argmax: Int
    Find the index of a maximal element.
    abstract def
    foreachActive(f: (Int, Double) ⇒ Unit): Unit
    Applies a function f to all the active elements of dense and sparse vector.
    abstract def
    numActives: Int
    Number of active entries.
    abstract def
    numNonzeros: Int
    Number of nonzero elements.
    abstract def
    size: Int
    Size of the vector.
    abstract def
    toArray: Array[Double]
    Converts the instance to a double array.
    abstract def
    toJson: String
    Converts the vector to a JSON string.
    abstract def
    toSparse: SparseVector
    Converts this vector to a sparse vector with all explicit zeros removed.
     */

    val sVectorOneMax = sVectorOne.argmax
    val sVectorOneNumNonZeros = sVectorOne.numNonzeros
    val sVectorOneSize = sVectorOne.size
    val sVectorOneArray = sVectorOne.toArray
    val sVectorOneJson = sVectorOne.toJson

    println("sVectorOneMax:" + sVectorOneMax)
    println("sVectorOneNumNonZeros:" + sVectorOneNumNonZeros)
    println("sVectorOneSize:" + sVectorOneSize)
    println("sVectorOneArray:" + sVectorOneArray)
    println("sVectorOneJson:" + sVectorOneJson)
    val dVectorOneToSparse = dVectorOne.toSparse

    println("dVectorOneToSparse:" + dVectorOneToSparse)


  }
}