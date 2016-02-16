/**
  * Created by Rajdeep on 12/22/15.
  */
import org.apache.spark.SparkContext

object RatingData {

  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "First Spark App")
    val rating_data_raw = sc.textFile("../../data/ml-100k/u.data")

    println(rating_data_raw.first())
    val num_ratings = rating_data_raw.count()

    val rating_data = rating_data_raw.map(line => line.split("\t"))
    println("rating_data.first() : " + rating_data.first())

    val ratings = rating_data.map(fields => fields(2).toInt)

    println("rating.take(10) : " + ratings.take(10))
    val max_rating = ratings.reduce( (x, y) => math.max(x, y))

    val min_rating = ratings.reduce( (x, y) => math.min(x, y))

    val mean_rating = ratings.reduce( (x, y) => x + y) / num_ratings.toFloat

    println("max_rating: " + max_rating)
    println("min_rating: " + min_rating)
    println("mean_rating: " + mean_rating)

    var user_data = sc.textFile("../../data/ml-100k/u.user")
    user_data = user_data.map(l => l.replaceAll("[|]", ","))
    val user_fields = user_data.map(l => l.split(","))
    val num_users = user_fields.map(l => l(0)).count()
    //val median_rating = math.median(ratings.collect()) function not supported - TODO
    val ratings_per_user = num_ratings / num_users
    val util = new Util()
    util.sc = sc
    val num_movies = util.getMovieDataRDD().count()
    val ratings_per_movie = num_ratings / num_movies

    val count_by_rating = ratings.countByValue()

    val user_ratings_grouped = rating_data.map(fields => (fields(0).toInt, fields(2).toInt)).groupByKey()
    //Python code : user_ratings_byuser = user_ratings_grouped.map(lambda (k, v): (k, len(v)))
    val user_ratings_byuser = user_ratings_grouped.map(v =>  (v._1,v._2.size))

    user_ratings_byuser.take(5)


    val user_ratings_byuser_local = user_ratings_byuser.map(v =>  v._2).collect()
    val movie_fields = util.movieFields()

    //TODO - Change the python code below
    //idx_bad_data = np.where(years_pre_processed_array==1900)[0][0]
    //years_pre_processed_array[idx_bad_data] = median_year

    //Feature Extraction
    //Categorical Features: _1-of-k_ Encoding of User Occupation

    val all_occupations = user_fields.map(fields=> fields(3)).distinct().collect()
    scala.util.Sorting.quickSort(all_occupations)

    var all_occupations_dict:Map[String, Int] = Map()
    var idx = 0;
    // for loop execution with a range
    for( idx <- 0 to (all_occupations.length -1)){
      all_occupations_dict += all_occupations(idx) -> idx
    }

    println("Encoding of 'doctor : " + all_occupations_dict("doctor"))
    println("Encoding of 'programmer' : " + all_occupations_dict("programmer"))

    sc.stop()
  }

  def mean( x:Array[Int]) : Int = {
    return x.sum/x.length
  }

  def median( x:Array[Int]) : Int = {
    val middle = x.length/2
    if (x.length%2 == 1) {
      return x(middle)
    } else {
      return (x(middle-1) + x(middle)) / 2
    }
  }
}