/**
  * Created by Rajdeep on 12/22/15.
  */
import org.apache.spark.SparkContext

object UserData {

  def main(args: Array[String]) {

    val sc = new SparkContext("local[2]", "First Spark App")
    var user_data = sc.textFile("../../data/ml-100k/u.user")

    println("user_data first" + user_data.first())
    user_data = user_data.map(l => l.replaceAll("[|]", ","))

    val user_fields = user_data.map(l => l.split(","))
    val num_users = user_fields.map(l => l(0)).count()
    val num_genders = user_fields.map(l => l(2)).distinct().count()

    val num_occupations = user_fields.map(l => l(3)).distinct().count()
    val num_zipcodes = user_fields.map(l => l(4)).distinct().count()

    println("num_users: " + num_users)
    println("num_genders: " + num_genders)
    println("num_occupations: " + num_occupations)
    println("num_zipcodes: " + num_zipcodes)

    val ages = user_fields.map( x => (x(1).toInt)).collect()

    val count_by_occupation = user_fields.map( fields => (fields(3), 1)).reduceByKey( (x, y) => x + y).collect()
    println("count_by_occupation: " + count_by_occupation)
    val count_by_occupation2 = user_fields.map( fields =>  fields(3)).countByValue()
    println("count_by_occupation2: " + count_by_occupation2)
    sc.stop()
  }
}