package com.sparksample

import org.apache.spark.SparkContext
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.mllib.recommendation.Rating

import scala.collection.mutable.ListBuffer

/**
 * A simple Spark app in Scala
 */
object MovieLensFPGrowthApp {

  val PATH = "../../data"
  def main(args: Array[String]) {

    val sc = new SparkContext("local[2]", "Chapter 5 App")
    val rawData = sc.textFile(PATH + "/ml-100k/u.data")
    rawData.first()

    /* Extract the user id, movie id and rating only from the dataset */
    val rawRatings = rawData.map(_.split("\t").take(3))
    rawRatings.first()
    val ratings = rawRatings.map { case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toDouble) }
    val ratingsFirst = ratings.first()
    println(ratingsFirst)

    val userId = 789
    val K = 10

    val movies = sc.textFile(PATH + "/ml-100k/u.item")
    val titles = movies.map(line => line.split("\\|").take(2)).map(array => (array(0).toInt, array(1))).collectAsMap()
    titles(123)

    var eRDD = sc.emptyRDD
    var z = Seq[String]()

    val l = ListBuffer()
    val aj = new Array[String](400)
    var i = 0
    for( a <- 501 to 900) {
      val moviesForUserX = ratings.keyBy(_.user).lookup(a)
      val moviesForUserX_10 = moviesForUserX.sortBy(-_.rating).take(10)
      val moviesForUserX_10_1 = moviesForUserX_10.map(r => r.product)
      var temp = ""
      for( x <- moviesForUserX_10_1){
        if(temp.equals(""))
          temp = x.toString
        else {
          temp =  temp + " " + x
        }
      }

      aj(i) = temp
      i += 1
    }
    z = aj

    val transaction = z.map(_.split(" "))

    val rddx = sc.parallelize(transaction, 2).cache()

    val fpg = new FPGrowth()
    val model = fpg
      .setMinSupport(0.1)
      .setNumPartitions(1)
      .run(rddx)

    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }
    sc.stop()
  }

}
