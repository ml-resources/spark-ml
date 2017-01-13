import org.apache.spark.{SparkConf}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession

object Word2VecMl {
  case class Record(name: String)

  def main(args: Array[String]) {
    val spConfig = (new SparkConf).setMaster("local").setAppName("SparkApp")
    val spark = SparkSession
      .builder
      .appName("Word2Vec example").config(spConfig)
      .getOrCreate()

    import spark.implicits._

    val rawDF = spark.sparkContext
      .wholeTextFiles("../data/20news-bydate-train/*")

    val textDF = rawDF.map(x => x._2.split(" ")).map(Tuple1.apply)
      .toDF("text")
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(textDF)
    val result = model.transform(textDF)
    result.select("result").take(3).foreach(println)
    val ds = model.findSynonyms("hockey", 20).select("word")
    ds.rdd.saveAsTextFile("./output/hockey-synonyms")
    ds.show()
    spark.stop()
  }
}
