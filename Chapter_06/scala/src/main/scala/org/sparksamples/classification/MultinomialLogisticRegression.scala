package org.sparksamples.classification

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

import scala.util.Random
import scala.collection.JavaConverters._
import scala.util.Random
import scala.util.control.Breaks._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.optimization._
import org.apache.spark.mllib.regression._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

/**
  * Created by manpreet.singh on 14/04/16.
  */
object MultinomialLogisticRegression {

  def generateMultinomialLogisticInput(
                                        weights: Array[Double],
                                        xMean: Array[Double],
                                        xVariance: Array[Double],
                                        addIntercept: Boolean,
                                        nPoints: Int,
                                        seed: Int): Seq[LabeledPoint] = {
    val rnd = new Random(seed)

    val xDim = xMean.length
    val xWithInterceptsDim = if (addIntercept) xDim + 1 else xDim
    val nClasses = weights.length / xWithInterceptsDim + 1

    val x = Array.fill[Vector](nPoints)(Vectors.dense(Array.fill[Double](xDim)(rnd.nextGaussian())))

    x.foreach { vector =>
      // This doesn't work if `vector` is a sparse vector.
      val vectorArray = vector.toArray
      var i = 0
      val len = vectorArray.length
      while (i < len) {
        vectorArray(i) = vectorArray(i) * math.sqrt(xVariance(i)) + xMean(i)
        i += 1
      }
    }

    val y = (0 until nPoints).map { idx =>
      val xArray = x(idx).toArray
      val margins = Array.ofDim[Double](nClasses)
      val probs = Array.ofDim[Double](nClasses)

      for (i <- 0 until nClasses - 1) {
        for (j <- 0 until xDim) margins(i + 1) += weights(i * xWithInterceptsDim + j) * xArray(j)
        if (addIntercept) margins(i + 1) += weights((i + 1) * xWithInterceptsDim - 1)
      }
      // Preventing the overflow when we compute the probability
      val maxMargin = margins.max
      if (maxMargin > 0) for (i <- 0 until nClasses) margins(i) -= maxMargin

      // Computing the probabilities for each class from the margins.
      val norm = {
        var temp = 0.0
        for (i <- 0 until nClasses) {
          probs(i) = math.exp(margins(i))
          temp += probs(i)
        }
        temp
      }
      for (i <- 0 until nClasses) probs(i) /= norm

      // Compute the cumulative probability so we can generate a random number and assign a label.
      for (i <- 1 until nClasses) probs(i) += probs(i - 1)
      val p = rnd.nextDouble()
      var y = 0
      breakable {
        for (i <- 0 until nClasses) {
          if (p < probs(i)) {
            y = i
            break
          }
        }
      }
      y
    }

    val testData = (0 until nPoints).map(i => LabeledPoint(y(i), x(i)))
    testData
  }

  /** 3 classes, 2 features */
  private val multiclassModel = new LogisticRegressionModel(
    weights = Vectors.dense(0.1, 0.2, 0.3, 0.4), intercept = 1.0, numFeatures = 2, numClasses = 3)

  def validatePrediction(
                          predictions: Seq[Double],
                          input: Seq[LabeledPoint],
                          expectedAcc: Double = 0.83) {
    val numOffPredictions = predictions.zip(input).count { case (prediction, expected) =>
      prediction != expected.label
    }
    // At least 83% of the predictions should be on.
    ((input.length - numOffPredictions).toDouble / input.length) > expectedAcc
  }

  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "Classification")

    val nPoints = 10000

    /**
      * The following weights and xMean/xVariance are computed from iris dataset with lambda = 0.2.
      * As a result, we are actually drawing samples from probability distribution of built model.
      */
    val weights = Array(
      -0.57997, 0.912083, -0.371077, -0.819866, 2.688191,
      -0.16624, -0.84355, -0.048509, -0.301789, 4.170682)

    val xMean = Array(5.843, 3.057, 3.758, 1.199)
    val xVariance = Array(0.6856, 0.1899, 3.116, 0.581)

    val testData = generateMultinomialLogisticInput(
      weights, xMean, xVariance, true, nPoints, 42)

    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()

    val lr = new LogisticRegressionWithLBFGS().setIntercept(true).setNumClasses(3)
    lr.optimizer.setConvergenceTol(1E-15).setNumIterations(200)

    val model = lr.run(testRDD)

    val numFeatures = testRDD.map(_.features.size).first()
    val initialWeights = Vectors.dense(new Array[Double]((numFeatures + 1) * 2))
    val model2 = lr.run(testRDD, initialWeights)

    val weightsR = Vectors.dense(Array(
      -0.5837166, 0.9285260, -0.3783612, -0.8123411, 2.6228269,
      -0.1691865, -0.811048, -0.0646380, -0.2919834, 4.1119745))

    val validationData = generateMultinomialLogisticInput(
      weights, xMean, xVariance, true, nPoints, 17)
    val validationRDD = sc.parallelize(validationData, 2)
    // The validation accuracy is not good since this model (even the original weights) doesn't have
    // very steep curve in logistic function so that when we draw samples from distribution, it's
    // very easy to assign to another labels. However, this prediction result is consistent to R.
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData, 0.47)

  }

}
