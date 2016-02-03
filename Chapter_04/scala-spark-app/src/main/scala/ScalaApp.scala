import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.io._
import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * A simple Spark app in Scala
 */
object ScalaApp {

  def main(args: Array[String]) {
    val file = new File("output-" + new Util().getDate() + ".log")
    val bw = new BufferedWriter(new FileWriter(file))
    val sc = new SparkContext("local[2]", "Chapter 4 App")
    val movies = sc.textFile("../../data/ml-100k/u.item")
    val titles = movies.map(line => line.split("\\|").take(2)).map(array => (array(0).toInt, array(1))).collectAsMap()
    //println(titles(123))
    bw.write("Title of Movie : 123 : " + titles(123))
    sc.stop()
    bw.close()
  }
  class Util {
    def getDate(): String = {
      val today = Calendar.getInstance().getTime()
      // (2) create a date "formatter" (the date format we want)
      val formatter = new SimpleDateFormat("yyyy-MM-dd-hh.mm.ss")
   
      // (3) create a new String using the date format we want
      val folderName = formatter.format(today)
      return folderName
    }
  }

}
