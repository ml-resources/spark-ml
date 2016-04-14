package org.sparksamples.classification

import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SQLContext

/**
  * Created by manpreet.singh on 14/04/16.
  */
object MultilayerPerceptronClassifierImpl {

  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "Classification")

    val sqlContext = new SQLContext(sc)

    // Load the data stored in LIBSVM format as a DataFrame.
    val data = sqlContext.read.format("libsvm")
      .load("/Users/manpreet.singh/Sandbox/codehub/github/machinelearning/breeze.io/src/main/scala/moviestream/classification/sample_multiclass_classification_data.txt")
    // Split the data into train and test
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 1234L)
    val train = splits(0)
    val test = splits(1)
    // specify layers for the neural network:
    // input layer of size 4 (features), two intermediate of size 5 and 4
    // and output of size 3 (classes)
    val layers = Array[Int](4, 5, 4, 3)
    // create the trainer and set its parameters
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(100)
    // train the model
    val model = trainer.fit(train)
    // compute precision on the test set
    val result = model.transform(test)
    val predictionAndLabels = result.select("prediction", "label")
    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("precision")
    println("Precision:" + evaluator.evaluate(predictionAndLabels))
  }

}
