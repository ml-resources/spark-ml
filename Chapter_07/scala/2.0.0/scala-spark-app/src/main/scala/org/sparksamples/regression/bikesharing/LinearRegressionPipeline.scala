package org.sparksamples.classification.stumbleupon

import org.apache.log4j.Logger
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.DataFrame

/**
  * Created by manpreet.singh on 01/05/16.
  */
object LinearRegressionPipeline {
  @transient lazy val logger = Logger.getLogger(getClass.getName)

  def linearRegressionPipeline(vectorAssembler: VectorAssembler, dataFrame: DataFrame) = {
    val lr = new LinearRegression()

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 0.25, 0.5, 0.75, 1.0))
      .build()

    val pipeline = new Pipeline().setStages(Array(vectorAssembler, lr))

    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(pipeline)
      .setEvaluator(new RegressionEvaluator)
      .setEstimatorParamMaps(paramGrid)
      // 80% of the data will be used for training and the remaining 20% for validation.
      .setTrainRatio(0.8)

    val Array(training, test) = dataFrame.randomSplit(Array(0.8, 0.2), seed = 12345)

    val model = trainValidationSplit.fit(training)
    val holdout = model.transform(test).select("prediction","label")

    val rm = new RegressionMetrics(holdout.rdd.map(x => (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))

    logger.info("Test Metrics")
    logger.info("Test Explained Variance:")
    logger.info(rm.explainedVariance)
    logger.info("Test R^2 Coef:")
    logger.info(rm.r2)
    logger.info("Test MSE:")
    logger.info(rm.meanSquaredError)
    logger.info("Test RMSE:")
    logger.info(rm.rootMeanSquaredError)

    val totalPoints = training.count()
    val lrTotalCorrect = holdout.rdd.map(x => if (x(0).asInstanceOf[Double] == x(1).asInstanceOf[Double]) 1 else 0).sum()
    val accuracy = lrTotalCorrect/totalPoints
    println("Accuracy of LinearRegression is: ", accuracy)
  }
}
