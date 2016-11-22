package org.sparksamples.regression.bikesharing

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * Created by manpreet.singh on 25/04/16.
  */
object BikeSharingExecutor {
  @transient lazy val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("BikeSharing")
      .master("local[1]")
      .getOrCreate()

    // read from csv
    val df = spark.read.format("csv").option("header", "true").load("/Users/manpreet.singh/Sandbox/codehub/github/machinelearning/spark-ml/Chapter_07/scala/2.0.0/scala-spark-app/src/main/scala/org/sparksamples/regression/dataset/BikeSharing/hour.csv")
    df.cache()

    df.registerTempTable("BikeSharing")
    print(df.count())

    spark.sql("SELECT * FROM BikeSharing").show()

    // drop record id, date, casual and registered columns
    val df1 = df.drop("instant").drop("dteday").drop("casual").drop("registered")

    // convert to double: season,yr,mnth,hr,holiday,weekday,workingday,weathersit,temp,atemp,hum,windspeed,casual,registered,cnt
    val df2 = df1.withColumn("season", df1("season").cast("double")).withColumn("yr", df1("yr").cast("double"))
      .withColumn("mnth", df1("mnth").cast("double")).withColumn("hr", df1("hr").cast("double")).withColumn("holiday", df1("holiday").cast("double"))
      .withColumn("weekday", df1("weekday").cast("double")).withColumn("workingday", df1("workingday").cast("double")).withColumn("weathersit", df1("weathersit").cast("double"))
      .withColumn("temp", df1("temp").cast("double")).withColumn("atemp", df1("atemp").cast("double")).withColumn("hum", df1("hum").cast("double"))
      .withColumn("windspeed", df1("windspeed").cast("double")).withColumn("label", df1("label").cast("double"))

    df2.printSchema()

    // drop label and create feature vector
    val df3 = df2.drop("label")
    val featureCols = df3.columns

    val vectorAssembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("rawFeatures")
    val vectorIndexer = new VectorIndexer().setInputCol("rawFeatures").setOutputCol("features").setMaxCategories(2)

    // set as an argument
    val command = "GBT_withoutCatg"

    executeCommand(command, vectorAssembler, vectorIndexer, df2, spark)
  }

  def executeCommand(arg: String, vectorAssembler: VectorAssembler, vectorIndexer: VectorIndexer, dataFrame: DataFrame, spark: SparkSession) = arg match {
    case "LR__withCatg" => LinearRegressionPipeline.linearRegressionWithVectorFormat(vectorAssembler, vectorIndexer, dataFrame)
    case "LR__withoutCatg" => LinearRegressionPipeline.linearRegressionWithSVMFormat(spark)

    case "GLR_withCatg" => GeneralizedLinearRegressionPipeline.genLinearRegressionWithVectorFormat(vectorAssembler, vectorIndexer, dataFrame)
    case "GLR_withoutCatg"=> GeneralizedLinearRegressionPipeline.genLinearRegressionWithSVMFormat(spark)

    case "DT_withCatg" => DecisionTreeRegressionPipeline.decTreeRegressionWithVectorFormat(vectorAssembler, vectorIndexer, dataFrame)
    case "DT_withoutCatg"=> GeneralizedLinearRegressionPipeline.genLinearRegressionWithSVMFormat(spark)

    case "RF_withCatg" => RandomForestRegressionPipeline.randForestRegressionWithVectorFormat(vectorAssembler, vectorIndexer, dataFrame)
    case "RF_withoutCatg"=> RandomForestRegressionPipeline.randForestRegressionWithSVMFormat(spark)

    case "GBT_withCatg" => GradientBoostedTreeRegressorPipeline.gbtRegressionWithVectorFormat(vectorAssembler, vectorIndexer, dataFrame)
    case "GBT_withoutCatg"=> GradientBoostedTreeRegressorPipeline.gbtRegressionWithSVMFormat(spark)

  }

  object DFHelper
  def castColumnTo( df: DataFrame, cn: String, tpe: DataType ) : DataFrame = {
    df.withColumn( cn, df(cn).cast(tpe) )
  }
}

