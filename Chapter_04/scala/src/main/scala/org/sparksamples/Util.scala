package org.sparksamples

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by ubuntu on 2/2/16.
  */
class Util {
  var sc:SparkContext = null

  def getMovieDataRDD(): RDD[String] = {
    val movie_data = sc.textFile("../../data/ml-100k/u.item")
    return movie_data
  }

  def numMovies() : Long = {
    return this.getMovieDataRDD().count()
  }

  def movieFields() : RDD[Array[String]] = {
    return this.getMovieDataRDD().map(lines =>  lines.split("\\|"))
  }

  def mean( x:Array[Int]) : Int = {
    return x.sum/x.length
  }

  def getUserData() : RDD[String] = {
    //val sc = new SparkContext("local[2]", "First Spark App")
    var user_data = sc.textFile("../../data/ml-100k/u.user")
    return user_data
  }

  def getUserFields() : RDD[Array[String]] = {
    val user_data = this.getUserData()
    val user_fields = user_data.map(l => l.split(","))
    return user_fields
  }


}
