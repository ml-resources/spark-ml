package org.sparksamples.classification.stumbleupon

import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

/**
  * Created by manpreet.singh on 01/05/16.
  */
object NaiveBayesPipeline {

  def naiveBayesPipeline(vectorAssembler: VectorAssembler, dataFrame: DataFrame) = {
    val Array(training, test) = dataFrame.randomSplit(Array(0.9, 0.1), seed = 12345)

    // Set up Pipeline
    val stages = new mutable.ArrayBuffer[PipelineStage]()

    val nb = new NaiveBayes()

    stages += vectorAssembler
    val pipeline = new Pipeline().setStages(stages.toArray)

    // Fit the Pipeline
    val startTime = System.nanoTime()
    val model = pipeline.fit(training)
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")

    val predictions = model.transform(test).select("prediction").rdd.map(_.getDouble(0))
    val labels = model.transform(test).select("label").rdd.map(_.getDouble(0))
    val accuracy = new MulticlassMetrics(predictions.zip(labels)).precision
    println(s"  Accuracy : $accuracy")
  }

}
