package org.sparksamples

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Rajdeep Dua
  */
object MovieLensDataKMeansClusteringPersist {
  val PATH= "../data/ml-100k"
  val OUTPUT = "output/"
  def main(args: Array[String]): Unit = {
    val spConfig = (new SparkConf).setMaster("local[1]").setAppName("SparkApp").
      set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(spConfig)
    val movies = sc.textFile(PATH + "/u.item")
    println(movies.first)
    val genres = sc.textFile(PATH + "/u.genre")
    genres.take(5).foreach(println)

    val genreMap = genres.filter(!_.isEmpty).map(line => line.split("\\|")).
      map(array => (array(1), array(0))).collectAsMap
    println(genreMap)

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
    println(titlesAndGenres.first)

    val rawData = sc.textFile(PATH + "/u.data")
    val rawRatings = rawData.map(_.split("\t").take(3))
    val ratings = rawRatings.map{ case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toDouble) }
    ratings.cache
    val alsModel = ALS.train(ratings, 50, 10, 0.1)
    import org.apache.spark.mllib.linalg.Vectors
    val movieFactors = alsModel.productFeatures.map { case (id, factor) => (id, Vectors.dense(factor)) }
    val movieVectors = movieFactors.map(_._2)
    val movie_vectors_reduced = movieVectors.map(x => Util.reduceDimension2(x))
    print(movie_vectors_reduced.count())
    print(movie_vectors_reduced.first())
    val time = new java.util.Date()
    val format = new java.text.SimpleDateFormat("dd-MM-yyyy-mm-ss")
    val timeFormatted = format.format(time)
    movie_vectors_reduced.saveAsTextFile(OUTPUT +"vectors-" + timeFormatted)

    import org.apache.spark.mllib.linalg.distributed.RowMatrix
    val movieMatrix = new RowMatrix(movieVectors)
    val movieMatrixSummary =
      movieMatrix.computeColumnSummaryStatistics()

    val numClusters = 5
    val numIterations = 10
    val numRuns = 3
    import org.apache.spark.mllib.clustering.KMeans

    val movieClusterModel = KMeans.train(movieVectors, numClusters, numIterations, numRuns)

    val movie1 = movieVectors.first
    val movieCluster = movieClusterModel.predict(movie1)
    println(movieCluster)

    val clusterOutput = movieVectors.map(x=> movieClusterModel.predict(x))
    clusterOutput.saveAsTextFile(OUTPUT + "cluster-membership-" + timeFormatted)
    println("done")

  }
}