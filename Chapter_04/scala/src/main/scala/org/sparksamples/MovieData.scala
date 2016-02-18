package org.sparksamples

/**
  * Created by Rajdeep on 12/22/15.
  */
import org.apache.spark.SparkContext

object MovieData {

  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "First Spark App")
    val movie_data = sc.textFile("../../data/ml-100k/u.item")
    println(movie_data.first())

    val num_movies = movie_data.count()
    println("num_movies: " + num_movies)

    def convertYear( x:String) : Int = {
      try
        return x.takeRight(4).toInt
      catch {
        case e: Exception => println("exception caught: " + e + " Returning 1900");
          return 1900
      }
    }

    val movie_fields = movie_data.map(lines =>  lines.split("\\|"))
    val years = movie_fields.map( field => field(2)).map( x => convertYear(x))
    val years_filtered = years.filter(x => (x != 1900) )
    val movie_ages = years_filtered.map(yr =>  (1998-yr) ).countByValue()

    val values = movie_ages.values
    val bins = movie_ages.keys

    println("movie_fields: " + movie_fields)
    println("years: " + years)
    println("years_filtered: " + years_filtered)
    println("movie_ages: " + movie_ages)
    println("values: " + values)
    println("bins: " + bins)

    sc.stop()
  }
}