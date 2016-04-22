import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.IsotonicRegression
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map
import scala.collection.mutable.ListBuffer

/**
 * A simple Spark app in Scala
 */
object IsotonicRegressionApp{

  def get_mapping(rdd :RDD[Array[String]], idx: Int) : Map[String, Long] = {
    return rdd.map( fields=> fields(idx)).distinct().zipWithIndex().collectAsMap()
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("GradientBoostedTreesRegressionApp")
    val sc = new SparkContext(conf)

    // we take the raw data in CSV format and convert it into a set of records
    // of the form (user, product, price)
    val rawData = sc.textFile("../data/hour_noheader.csv")
    val numData = rawData.count()
    val records = rawData.map(line => line.split(","))
    records.cache()
    //print("Mapping of first categorical feature column: " +  get_mapping(records, 2))
    var list = new ListBuffer[Map[String, Long]]()
    for( i <- 2 to 9){
      val m = get_mapping(records, i)
      list += m
    }
    val mappings = list.toList
    var catLen = 0
    mappings.foreach( m => (catLen +=m.size))

    val numLen = records.first().slice(11, 15).size
    val totalLen = catLen + numLen


    val data = {
      records.map(r => LabeledPoint(Util.extractLabel(r), Util.extractFeatures(r, catLen, mappings)))
    }
    val parsedData = records.map { r =>
      (Util.extractLabel(r), Util.extractAvgFeature(r, catLen, mappings), 1.0)
    }
    /*val parsedData2 = records.map { r =>
      Util.extractFeatures(r, catLen, mappings)
    }
    println("parsedData2:" + parsedData2.count())
    val mat = new RowMatrix(parsedData2)
    // Compute principal components.
    val pc = mat.computePrincipalComponents(20)
    //val svd = mat.computeSVD(20,computeU = true)
    print(pc.numCols)
    print(pc.numRows)
    import org.apache.spark.mllib.linalg._
    def toRDD(m: Matrix): RDD[Vector] = {
      val columns = m.toArray.grouped(m.numRows)
      val rows = columns.toSeq.transpose // Skip this if you want a column-major RDD.
      val vectors = rows.map(row => new DenseVector(row.toArray))
      sc.parallelize(vectors)
    }
    val pcaRDD = toRDD(pc)
    pcaRDD.foreach(println)*/

    /*val U: RowMatrix = svd.U // The U factor is a RowMatrix.
    val s: Vector = svd.s // The singular values are stored in a local dense vector.
    val V: Matrix = svd.V*/
    //val first_point = data.first()
    //println("Gradient Boosted Trees Model feature vector:" + first_point.features.toString)
    //println("Gradient Boosted Trees Model feature vector length: " + first_point.features.size)

    val iterations = 10
    val step = 0.1
    val intercept =false

    val x = new IsotonicRegression().setIsotonic(true)
    val model = x.run(parsedData)

    val parsedData1: RDD[Double] = parsedData.map(r => r._2)
    //val model = GradientBoostedTrees.train(data, boostingStrategy)
    val true_vs_predicted = parsedData1.map(p => (p, model.predict(p)))
    //val true_vs_predicted = data.map(p => (p.label, model.predict(parsedData1)))
    val true_vs_predicted_take5 = true_vs_predicted.take(5)
    for(i <- 0 until 4) {
      println("True vs Predicted: " + "i :" + true_vs_predicted_take5(i))
    }
    val mse = true_vs_predicted.map{ case(t, p) => Util.squaredError(t, p)}.mean()
    val mae = true_vs_predicted.map{ case(t, p) => Util.absError(t, p)}.mean()
    val rmsle = Math.sqrt(true_vs_predicted.map{ case(t, p) => Util.squaredLogError(t, p)}.mean())

    println("Isotonic Regression - Mean Squared Error: "  + mse)
    println("Isotonic Regression  - Mean Absolute Error: " + mae)
    println("Isotonic Regression  - Root Mean Squared Log Error:" + rmsle)
    sc.stop()
  }
}
