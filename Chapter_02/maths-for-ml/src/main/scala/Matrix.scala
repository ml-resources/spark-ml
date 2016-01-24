import breeze.linalg.{DenseMatrix, DenseVector}


object MatrixOperations {

  def main(args: Array[String]) {
    val a = DenseMatrix((1,2),(3,4))
    val b = DenseMatrix((2,2),(2,2))
    val c = a + b
    println("a + b : \n" + c)
    val d = a*b
    println("Dot product a*b : \n" + d)

    val e = 	a :+= 1
    val f = a :< b

    println("Inplace Addition : \n" + e)

    /*
    Inplace addition	a :+= 1.0
    Inplace elementwise multiplication	a :*= 2.0
    Vector dot product	a dot b,a.t * b†	dot(a,b)	dot(a,b)
    Elementwise sum	sum(a)
    Elementwise max	a.max
    Elementwise argmax	argmax(a)
    Ceiling	ceil(a)
    Floor	floor(a)
    */

  }

}
