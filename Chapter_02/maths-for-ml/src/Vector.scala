import breeze.linalg.{SparseVector, DenseVector, DenseMatrix}

/**
  * Created by manpreet.singh on 23/01/16.
  */
object Vector {

  def main(args: Array[String]) {
    // dense vector
    val dv = DenseVector(2f, 0f, 3f, 2f, -1f)
    dv.update(3, 6f)
    println(dv)

    // sparse vector
    val sv:SparseVector[Double] = SparseVector(5)()
    sv(0) = 1
    sv(2) = 3
    sv(4) = 5
    val m:SparseVector[Double] = sv.mapActivePairs((i,x) => x+1)
    println(m)

  }

}
