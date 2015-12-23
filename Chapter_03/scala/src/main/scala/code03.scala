/**
  * Created by ubuntu on 12/22/15.
  */
import org.apache.spark.SparkContext

object code03 {

  def main(args: Array[String]) {

    val sc = new SparkContext("local[2]", "First Spark App")

    // we take the raw data in CSV format and convert it into a set of records of the form (user, product, price)
    var user_data = sc.textFile("../../data/ml-100k/u.user")

    println(">>>>>>>>>>>>" + user_data.first())
    user_data = user_data.map(l => l.replaceAll("[|]", ","))
    //  1|24|M|technician|85711

    val user_fields = user_data.map(l => l.split(","))

    val num_users = user_fields.map(l => l(0)).count()

    val num_genders = user_fields.map(l => l(2)).distinct().count()

    val num_occupations = user_fields.map(l => l(3)).distinct().count()
    val num_zipcodes = user_fields.map(l => l(4)).distinct().count()
    println("num_users" + num_users)
    sc.stop()
  }
}