package org.sparksamples

import org.apache.spark.mllib.linalg.Vector

/**
  * Created by Rajdeep Dua on 6/12/16.
  */
object Util {
  val PATH = "/home/ubuntu/work/spark-2.0.0-bin-hadoop2.7/"
  def reduceDimension2(x: Vector) : String= {
    var i = 0
    var l = x.toArray.size
    var l_2 = l/2.toInt
    var x_ = 0.0
    var y_ = 0.0

    for(i <- 0 until l_2) {
      x_ += x(i).toDouble
    }
    for(i <- (l_2 + 1) until l) {
      y_ += x(i).toDouble
    }
    var t = x_ + "," + y_
    return t
  }

}
