package org.sparksamples

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Rajdeep Dua
  */
object MovieLensDataBisectingKMeansClustering {
  val PATH= "../data/ml-100k"
  def main(args: Array[String]): Unit = {
    val spConfig = (new SparkConf).setMaster("local[1]").setAppName("SparkApp").
      set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(spConfig)
    //val path = PATH + "../data/"
    //val rdd = sc.wholeTextFiles(path)
    val movies = sc.textFile(PATH + "/u.item")
    println(movies.first)
    val genres = sc.textFile(PATH + "/u.genre")
    genres.take(5).foreach(println)

    val genreMap = genres.filter(!_.isEmpty).map(line => line.split("\\|")).
      map(array => (array(1), array(0))).collectAsMap


    val titlesAndGenres = movies.map(_.split("\\|")).map { array =>
      val genres = array.toSeq.slice(5, array.size)
      val genresAssigned = genres.zipWithIndex.filter { case (g, idx)
      =>
        g == "1"
      }.map { case (g, idx) =>
        genreMap(idx.toString)
      }
      (array(0).toInt, (array(1), genresAssigned))
    }

    val rawData = sc.textFile(PATH + "/u.data")
    val rawRatings = rawData.map(_.split("\t").take(3))
    val ratings = rawRatings.map{ case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toDouble) }
    ratings.cache
    val alsModel = ALS.train(ratings, 50, 10, 0.1)
    import org.apache.spark.mllib.linalg.Vectors
    val movieFactors = alsModel.productFeatures.map { case (id, factor) => (id, Vectors.dense(factor)) }
    val movieVectors = movieFactors.map(_._2)
    val userFactors = alsModel.userFeatures.map { case (id, factor) => (id, Vectors.dense(factor)) }
    val userVectors = userFactors.map(_._2)


    val numClusters = 5
    val numIterations = 10
    val numRuns = 3
    import org.apache.spark.mllib.clustering.BisectingKMeans
    val bKMeans = new BisectingKMeans()
    bKMeans.setMaxIterations(10)
    bKMeans.setMinDivisibleClusterSize(5)
    val movieClusterModel = bKMeans.run(movieVectors)

    val movie1 = movieVectors.first
    val movieCluster = movieClusterModel.predict(movie1)
    println(movieCluster)

    val userClusterModel = bKMeans.run(userVectors)

    val movieCost = movieClusterModel.computeCost(movieVectors)
    val userCost = userClusterModel.computeCost(userVectors)
    println("Bisecting KMeans WCSS for movies: " + movieCost)
    println("Bisecting KMeans WCSS for users: " + userCost)

    movieClusterModel.clusterCenters.zipWithIndex.foreach { case (center, idx) =>
      println(s"Cluster Center ${idx}: ${center}")
    }
    println("done")


  }
}