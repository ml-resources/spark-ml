package org.sparksamples.classification.stumbleupon

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by manpreet.singh on 25/04/16.
  */
object StumbleUponPipeline {
  @transient lazy val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) {
    val conf = SparkCommonUtils.createSparkConf("StumbleUpon")
    val sc = new SparkContext(conf)

    // create sql context
    val sqlContext = new SQLContext(sc)

    // get dataframe
    val df = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "true")
      .option("inferSchema", "true").load("/Users/manpreet.singh/Sandbox/codehub/github/machinelearning/breeze.io/src/main/scala/sparkMLlib/dataset/stumbleupon/train.tsv")

    // pre-processing
    df.registerTempTable("StumbleUpon")
    df.printSchema()
    sqlContext.sql("SELECT * FROM StumbleUpon WHERE alchemy_category = '?'").show()

    // convert type of '4 to n' columns to double
    val df1 = df.withColumn("avglinksize", df("avglinksize").cast("double"))
      .withColumn("commonlinkratio_1", df("commonlinkratio_1").cast("double"))
      .withColumn("commonlinkratio_2", df("commonlinkratio_2").cast("double"))
      .withColumn("commonlinkratio_3", df("commonlinkratio_3").cast("double"))
      .withColumn("commonlinkratio_4", df("commonlinkratio_4").cast("double"))
      .withColumn("compression_ratio", df("compression_ratio").cast("double"))
      .withColumn("embed_ratio", df("embed_ratio").cast("double"))
      .withColumn("framebased", df("framebased").cast("double"))
      .withColumn("frameTagRatio", df("frameTagRatio").cast("double"))
      .withColumn("hasDomainLink", df("hasDomainLink").cast("double"))
      .withColumn("html_ratio", df("html_ratio").cast("double"))
      .withColumn("image_ratio", df("image_ratio").cast("double"))
      .withColumn("is_news", df("is_news").cast("double"))
      .withColumn("lengthyLinkDomain", df("lengthyLinkDomain").cast("double"))
      .withColumn("linkwordscore", df("linkwordscore").cast("double"))
      .withColumn("news_front_page", df("news_front_page").cast("double"))
      .withColumn("non_markup_alphanum_characters", df("non_markup_alphanum_characters").cast("double"))
      .withColumn("numberOfLinks", df("numberOfLinks").cast("double"))
      .withColumn("numwords_in_url", df("numwords_in_url").cast("double"))
      .withColumn("parametrizedLinkRatio", df("parametrizedLinkRatio").cast("double"))
      .withColumn("spelling_errors_ratio", df("spelling_errors_ratio").cast("double"))
      .withColumn("label", df("label").cast("double"))
    df1.printSchema()

    // user defined function for cleanup of ?
    val replacefunc = udf {(x:Double) => if(x == "?") 0.0 else x}

    val df2 = df1.withColumn("avglinksize", replacefunc(df1("avglinksize")))
      .withColumn("commonlinkratio_1", replacefunc(df1("commonlinkratio_1")))
      .withColumn("commonlinkratio_2", replacefunc(df1("commonlinkratio_2")))
      .withColumn("commonlinkratio_3", replacefunc(df1("commonlinkratio_3")))
      .withColumn("commonlinkratio_4", replacefunc(df1("commonlinkratio_4")))
      .withColumn("compression_ratio", replacefunc(df1("compression_ratio")))
      .withColumn("embed_ratio", replacefunc(df1("embed_ratio")))
      .withColumn("framebased", replacefunc(df1("framebased")))
      .withColumn("frameTagRatio", replacefunc(df1("frameTagRatio")))
      .withColumn("hasDomainLink", replacefunc(df1("hasDomainLink")))
      .withColumn("html_ratio", replacefunc(df1("html_ratio")))
      .withColumn("image_ratio", replacefunc(df1("image_ratio")))
      .withColumn("is_news", replacefunc(df1("is_news")))
      .withColumn("lengthyLinkDomain", replacefunc(df1("lengthyLinkDomain")))
      .withColumn("linkwordscore", replacefunc(df1("linkwordscore")))
      .withColumn("news_front_page", replacefunc(df1("news_front_page")))
      .withColumn("non_markup_alphanum_characters", replacefunc(df1("non_markup_alphanum_characters")))
      .withColumn("numberOfLinks", replacefunc(df1("numberOfLinks")))
      .withColumn("numwords_in_url", replacefunc(df1("numwords_in_url")))
      .withColumn("parametrizedLinkRatio", replacefunc(df1("parametrizedLinkRatio")))
      .withColumn("spelling_errors_ratio", replacefunc(df1("spelling_errors_ratio")))
      .withColumn("label", replacefunc(df1("label")))

    // drop first 4 columns
    val df3 = df2.drop("url").drop("urlid").drop("boilerplate").drop("alchemy_category").drop("alchemy_category_score")

    // fill null values with
    val df4 = df3.na.fill(0.0)

    df4.registerTempTable("StumbleUpon_PreProc")
    df4.printSchema()
    sqlContext.sql("SELECT * FROM StumbleUpon_PreProc").show()

    // setup pipeline
    val assembler = new VectorAssembler()
      .setInputCols(Array("avglinksize", "commonlinkratio_1", "commonlinkratio_2", "commonlinkratio_3", "commonlinkratio_4", "compression_ratio"
        , "embed_ratio", "framebased", "frameTagRatio", "hasDomainLink", "html_ratio", "image_ratio"
        ,"is_news", "lengthyLinkDomain", "linkwordscore", "news_front_page", "non_markup_alphanum_characters", "numberOfLinks"
        ,"numwords_in_url", "parametrizedLinkRatio", "spelling_errors_ratio"))
      .setOutputCol("features")

    // val featuress = assembler.transform(df4)

    val lr = new LogisticRegression()

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 0.25, 0.5, 0.75, 1.0))
      .build()

    val pipeline = new Pipeline().setStages(Array(assembler, lr))

    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(pipeline)
      .setEvaluator(new RegressionEvaluator)
      .setEstimatorParamMaps(paramGrid)
      // 80% of the data will be used for training and the remaining 20% for validation.
      .setTrainRatio(0.8)

    val Array(training, test) = df4.randomSplit(Array(0.8, 0.2), seed = 12345)
    val model = trainValidationSplit.fit(training)

    val holdout = model.transform(test).select("prediction","label")

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

  }

  def savePredictions(predictions:DataFrame, testRaw:DataFrame, filePath:String) = {
    val tdOut = testRaw
      .select("Id")
      .distinct()
      .join(predictions, testRaw("Id") === predictions("PredId"), "outer")
      .select("Id", "Sales")
      .na.fill(0:Double) // some of our inputs were null so we have to
    // fill these with something
    tdOut
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save(filePath)
  }


  object DFHelper
  def castColumnTo( df: DataFrame, cn: String, tpe: DataType ) : DataFrame = {
    df.withColumn( cn, df(cn).cast(tpe) )
  }
}

