package org.sparksamples.classification.stumbleupon

import org.apache.log4j.Logger
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.mllib.evaluation.{MulticlassMetrics, RegressionMetrics}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

/**
  * Created by manpreet.singh on 01/05/16.
  */
object NaiveBayesPipeline {
  @transient lazy val logger = Logger.getLogger(getClass.getName)
  val completeData = true
  def naiveBayesPipeline(vectorAssembler: VectorAssembler, dataFrame: DataFrame) = {
    val Array(training, test) = dataFrame.randomSplit(Array(0.9, 0.1), seed = 12345)

    // Set up Pipeline
    val stages = new mutable.ArrayBuffer[PipelineStage]()

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
    stages += labelIndexer

    val nb = new NaiveBayes()

    stages += vectorAssembler
    stages += nb
    val pipeline = new Pipeline().setStages(stages.toArray)

    // Fit the Pipeline
    val startTime = System.nanoTime()
    val model = pipeline.fit(training)
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")

    val holdout = model.transform(test).select("prediction","label")
    val holdout_complete = model.transform(dataFrame).select("prediction")

    // have to do a type conversion for RegressionMetrics
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

    val predictions = model.transform(test).select("prediction").rdd.map(_.getDouble(0))
    val predictions_complete = model.transform(dataFrame).select("prediction").rdd.map(_.getDouble(0))
    val labels = model.transform(test).select("label").rdd.map(_.getDouble(0))
    val accuracy = new MulticlassMetrics(predictions.zip(labels)).precision
    println(s"  Accuracy : $accuracy")

    if (!completeData){
      savePredictions(holdout, test, rm,
        SparkConstants.PATH + "/results/NaiveBayes-results.csv")
    } else {
      saveOnlyPredictions(holdout_complete, rm,
        SparkConstants.PATH + "/results/NaiveBayes-results-complete.csv")
    }


  }

  def savePredictions(predictions:DataFrame, testRaw:DataFrame, regressionMetrics: RegressionMetrics, filePath:String) = {
    predictions
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save(filePath)
  }
  def saveOnlyPredictions(predictions:DataFrame, regressionMetrics: RegressionMetrics, filePath:String) = {
    predictions
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "false")
      .save(filePath)
  }
}
