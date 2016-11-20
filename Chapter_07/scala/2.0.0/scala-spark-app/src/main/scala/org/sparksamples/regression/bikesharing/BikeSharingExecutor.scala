package org.sparksamples.classification.stumbleupon

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}


/**
  * Created by manpreet.singh on 25/04/16.
  */
object BikeSharingExecutor {
  @transient lazy val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) {
    val conf = SparkCommonUtils.createSparkConf("BikeSharing")
    val sc = new SparkContext(conf)

    // create sql context
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.format("csv").option("header", "true").load("/Users/manpreet.singh/Sandbox/codehub/github/machinelearning/spark-ml/Chapter_07/scala/2.0.0/scala-spark-app/src/main/scala/org/sparksamples/regression/dataset/BikeSharing/hour.csv")
    df.cache()

    df.registerTempTable("BikeSharing")
    print(df.count())

    sqlContext.sql("SELECT * FROM BikeSharing").show()

    // drop record id, date, casual and registered columns
    val df1 = df.drop("instant").drop("dteday").drop("casual").drop("registered")

    // convert to double: season,yr,mnth,hr,holiday,weekday,workingday,weathersit,temp,atemp,hum,windspeed,casual,registered,cnt
    val df2 = df1.withColumn("season", df1("season").cast("double")).withColumn("yr", df1("yr").cast("double"))
      .withColumn("mnth", df1("mnth").cast("double")).withColumn("hr", df1("hr").cast("double")).withColumn("holiday", df1("holiday").cast("double"))
      .withColumn("weekday", df1("weekday").cast("double")).withColumn("workingday", df1("workingday").cast("double")).withColumn("weathersit", df1("weathersit").cast("double"))
      .withColumn("temp", df1("temp").cast("double")).withColumn("atemp", df1("atemp").cast("double")).withColumn("hum", df1("hum").cast("double"))
      .withColumn("windspeed", df1("windspeed").cast("double")).withColumn("label", df1("label").cast("double"))

    df2.printSchema()

    val df3 = df2.drop("label")
    val featureCols = df3.columns

    val vectorAssembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val vectorIndexer = new VectorIndexer().setInputCol("rawFeatures").setOutputCol("features").setMaxCategories(4)

    val command = args(0)

    executeCommand(command, vectorAssembler, df2, sc)
  }

  def executeCommand(arg: String, vectorAssembler: VectorAssembler, dataFrame: DataFrame, sparkContext: SparkContext) = arg match {
    case "LR" => LinearRegressionPipeline.linearRegressionPipeline(vectorAssembler, dataFrame)

    case "DT" => DecisionTreePipeline.decisionTreePipeline(vectorAssembler, dataFrame)

    case "RF" => RandomForestPipeline.randomForestPipeline(vectorAssembler, dataFrame)

    case "GBT" => GradientBoostedTreePipeline.gradientBoostedTreePipeline(vectorAssembler, dataFrame)

    case "NB" => NaiveBayesPipeline.naiveBayesPipeline(vectorAssembler, dataFrame)

    case "SVM" => SVMPipeline.svmPipeline(sparkContext)
  }
  
  object DFHelper
  def castColumnTo( df: DataFrame, cn: String, tpe: DataType ) : DataFrame = {
    df.withColumn( cn, df(cn).cast(tpe) )
  }
}

