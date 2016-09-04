package com.spark.recommendation

import org.apache.spark.SparkConf
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

/**
  * Created by manpreet.singh on 04/09/16.
  */
object FeatureExtraction {

  def main(args: Array[String]) {
    /**
      * In earlier versions of spark, spark context was entry point for Spark. As RDD was main API, it was created and manipulated using context API’s.
      * For every other API,we needed to use different contexts.For streaming, we needed StreamingContext, for SQL sqlContext and for hive HiveContext.
      * But as DataSet and Dataframe API’s are becoming new standard API’s we need an entry point build for them.
      * So in Spark 2.0, we have a new entry point for DataSet and Dataframe API’s called as Spark Session.
      * SparkSession is essentially combination of SQLContext, HiveContext and future StreamingContext.
      * All the API’s available on those contexts are available on spark session also. Spark session internally has a spark context for actual computation.
      */
    val spark = SparkSession.builder.master("local[2]").appName("FeatureExtraction").getOrCreate()

    val ratings = spark.read.textFile("/Users/manpreet.singh/Sandbox/codehub/github/machinelearning/spark-ml/Chapter_05/data/ml-100k 2/u.data")//.map(parseRating)
    println(ratings.first())
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
    println(training.first())

    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    val model = als.fit(training)
  }

  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }
}