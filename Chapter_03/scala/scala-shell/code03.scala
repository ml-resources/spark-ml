
/*******************/
/* User Dataset  */
/*******************/
var user_data = sc.textFile("../../data/ml-100k/u.user")
println(user_data.first())


//  1|24|M|technician|85711

val user_fields = user_data.map(l => l.split("\\|"))

val num_users = user_fields.map(l => l(0)).count()

val num_genders = user_fields.map(l => l(2)).distinct().count()

val num_occupations = user_fields.map(l => l(3)).distinct().count()
val num_zipcodes = user_fields.map(l => l(4)).distinct().count()

val ages = user_fields.map( x => (x(1).toInt)).collect()

val count_by_occupation = user_fields.map( fields => (fields(3), 1)).reduceByKey( (x, y) => x + y).collect()

/*
 * count_by_occupation: Array[(String, Int)] = Array((marketing,26), (librarian,51), (technician,27), (scientist,31),
 *(none,9), (executive,32), (other,105), (programmer,66), (lawyer,12), (entertainment,18), (salesman,12), (retired,14),
 * (healthcare,16), (administrator,79), (student,196), (doctor,7), (writer,45), (engineer,67), (homemaker,7),
 *(educator,95), (artist,28))
 */

val count_by_occupation2 = user_fields.map( fields =>  fields(3)).countByValue()

/*
count_by_occupation2: scala.collection.Map[String,Long] = Map(scientist -> 31, student -> 196, writer -> 45,
salesman -> 12, retired -> 14, administrator -> 79, programmer -> 66, doctor -> 7, homemaker -> 7, executive -> 32,
engineer -> 67, entertainment -> 18, marketing -> 26, technician -> 27, artist -> 28, librarian -> 51, lawyer -> 12, e
ducator -> 95, healthcare -> 16, none -> 9, other -> 105)
 */

/*******************/
/* Movies Dataset  */
/*******************/


var movie_data = sc.textFile("../../data/ml-100k/u.item")
println(movie_data.first())

val num_movies = movie_data.count()



/*
python code
def convert_year(x):
    try:
        return int(x[-4:])
    except:
        return 1900
        # there is a 'bad' data point with a blank year,
        # which we set to 1900 and will filter out later

*/
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

/*
movie_ages: scala.collection.Map[Int,Long] =
Map(
0 -> 65, 5 -> 126, 10 -> 11,
56 -> 2, 42 -> 4, 24 -> 8, 37 -> 3, 25 -> 4, 52 -> 5, 14 -> 8, 20 -> 4,
46 -> 3, 57 -> 5, 29 -> 4, 61 -> 4, 1 -> 286, 6 -> 37, 60 -> 3, 28 -> 3,
38 -> 5, 21 -> 4, 33 -> 5, 65 -> 2, 9 -> 15, 53 -> 4, 13 -> 7, 41 -> 8,
 2 -> 355, 32 -> 2, 34 -> 2, 45 -> 2, 64 -> 4, 17 -> 12, 22 -> 5,
 44 -> 7, 59 -> 7, 27 -> 7, 12 -> 15, 54 -> 5, 49 -> 4, 76 -> 1,
 7 -> 22, 39 -> 4, 66 -> 1, 3 -> 219, 35 -> 6, 48 -> 7, 63 -> 4,
 18 -> 8, 50 -> 3, 67 -> 1, 16 -> 13, 31 -> 5, 11 -> 13, 72 -> 1,
 43 -> 5, 40 -> 9, 26 -> 3, 55 -> 4, 23 -> 6, 8 -> 24, 58 -> 8,
 36 -> 5, 30 -> 6, 51 -> 5, 19 -> 9, 4 -> 214, 47 -> 5, 15 -> 5,
 68 -> 1, 62 -> 2)
 */

val values = movie_ages.values
val bins = movie_ages.keys

/*
  scala> val values = movie_ages.values
  values: Iterable[Long] =
  MapLike(65, 126, 11, 2, 4, 8, 3, 4, 5, 8, 4, 3, 5, 4, 4, 286,
  37, 3, 3, 5, 4, 5, 2, 15, 4, 7, 8, 355, 2, 2, 2, 4, 12, 5, 7,
  7, 7, 15, 5, 4, 1, 22, 4, 1, 219, 6, 7, 4, 8, 3, 1, 13, 5, 13,
   1, 5, 9, 3, 4, 6, 24, 8, 5, 6, 5, 9, 214, 5, 5, 1, 2)

  scala> val bins = movie_ages.keys
  bins: Iterable[Int] = Set(0, 5, 10, 56, 42, 24, 37, 25, 52, 14,
  20, 46, 57, 29, 61, 1, 6, 60, 28, 38, 21, 33, 65, 9, 53, 13, 41,
  2, 32, 34, 45, 64, 17, 22, 44, 59, 27, 12, 54, 49, 76, 7, 39, 66,
  3, 35, 48, 63, 18, 50, 67, 16, 31, 11, 72, 43, 40, 26, 55, 23, 8,
  58, 36, 30, 51, 19, 4, 47, 15, 68, 62)

*/

/* Rating */

val rating_data_raw = sc.textFile("../../data/ml-100k/u.data")
//res8: String = 196	242	3	881250949
println(rating_data_raw.first())
val num_ratings = rating_data_raw.count()
//num_ratings: Long = 100000

val rating_data = rating_data_raw.map(line => line.split("\t"))
rating_data.first()
//res10: Array[String] = Array(196, 242, 3, 881250949)

val ratings = rating_data.map(fields => fields(2).toInt)
ratings.take(10)
//res12: Array[Int] = Array(3, 3, 1, 2, 1, 4, 2, 5, 3, 3)
val max_rating = ratings.reduce( (x, y) => math.max(x, y))
//max_rating: Int = 5
val min_rating = ratings.reduce( (x, y) => math.min(x, y))
//min_rating: Int = 1

val mean_rating = ratings.reduce( (x, y) => x + y) / num_ratings.toFloat
//mean_rating: Float = 3.52986

//val median_rating = math.median(ratings.collect()) function not supported - TODO
val ratings_per_user = num_ratings / num_users
//ratings_per_user: Long = 106

//
val ratings_per_movie = num_ratings / num_movies
//ratings_per_movie: Long = 59

val count_by_rating = ratings.countByValue()
//count_by_rating: scala.collection.Map[Int,Long] = Map(5 -> 21201, 1 -> 6110, 2 -> 11370, 3 -> 27145, 4 -> 34174)

val user_ratings_grouped = rating_data.map(fields => (fields(0).toInt, fields(2).toInt)).groupByKey()
/*
res13: Array[(Int, Iterable[Int])] = Array((778, CompactBuffer(2, 1, 4, 3, 4, 4, 4, 1, 4, 2, 1, 4, 3, 3, 4, 4, 3, 1, 4,
 1, 4, 4, 3, 3, 4, 4, 3, 5, 2, 3, 5, 1, 5, 4, 3, 3, 5, 2, 2, 4, 2, 1, 4, 3, 3, 3, 2, 2, 3, 5, 1, 3, 4, 1, 2, 3, 3, 4,
 3, 1, 5, 2, 3, 1, 2)), (386, CompactBuffer(3, 3, 4, 2, 3, 4, 3, 5, 4, 3, 5, 4, 4, 3, 4, 5, 3, 4, 4, 5, 3, 3, 3)),
  (454, CompactBuffer(2, 4, 3, 3, 2, 3, 4, 4, 3, 4, 2, 4, 4, 3, 3, 2, 4, 4, 4, 3, 1, 2, 3, 3, 3, 3, 2, 2, 4, 3, 2, 2, 2,
   4, 2, 2, 3, 2, 3, 2, 3, 3, 4, 2, 1, 4, 4, 4, 4, 2, 4, 4, 3, 2, 4, 3, 3, 5, 2, 4, 2, 4, 2, 4, 2, 4, 2, 4, 4, 3, 3, 4,
    4, 4, 2, 2, 4, 3, 3, 3, 4, 3, 3, 4, 5, 4, 4, 5, 4, 3, 3, 4, 4, 4, 4, 4, 4, 4, 1, 2, 2, 4, 2, 4, 4, 4, 2, 2, 4, 3,
    3, 3, 1, 4, 2, 3, 3, 3, 3, 2, 2, 3, 2, 3, 2, 4, 2, 2, 3, 4, 1, 3, 4, 4, 3, 4, 3, 3, 2, 2, 4, 4, 4, ...
*/

//Python code : user_ratings_byuser = user_ratings_grouped.map(lambda (k, v): (k, len(v)))

val user_ratings_byuser = user_ratings_grouped.map(v =>  (v._1,v._2.size))
//user_ratings_byuser: org.apache.spark.rdd.RDD[(Int, Int)] = MapPartitionsRDD[15] at map at <console>:27

user_ratings_byuser.take(5)
//res22: Array[(Int, Int)] = Array((778,65), (386,23), (454,236), (772,33), (324,66))

val user_ratings_byuser_local = user_ratings_byuser.map(v =>  v._2).collect()

/*
Output :
  user_ratings_byuser_local: Array[Int] = Array(65, 23, 236, 33, 66, 63, 154, 73, 27, 166, 55, 53, 193, 98, 101, 68, 23,
  53, 21, 29, 21, 42, 194, 31, 50, 174, 174, 51, 36, 26, 21, 162, 27, 400, 206, 35, 540, 120, 55, 21, 36, 33, 128, 68, 50,
  155, 38, 23, 357, 27, 53, 29, 32, 35, 20, 44, 154, 35, 48, 134, 39, 20, 56, 75, 39, 306, 150, 127, 103, 131, 124, 27,
  43, 43, 195, 34, 388, 35, 23, 190, 54, 166, 21, 137, 24, 24, 21, 206, 22, 47, 242, 217, 68, 61, 360, 44, 232, 82, 57,
  187, 50, 21, 65, 20, 224, 33, 145, 49, 24, 63, 39, 33, 332, 59, 177, 66, 64, 163, 117, 29, 20, 21, 24, 327, 32, 89,
  226, 311, 29, 399, 60, 106, 45, 358, 239, 124, 39, 75, 38, 68, 342, 22, 27, 20, 207, 33, 34, 225, 109, 333, 42, 141,
  24, 26, 43, 119, 20, 27, 163, 236, 20, 27, 317, 28, 47, 59, 26, 41, 375, 129, 23, 149, 4...

 */

// Filling in Bad or Missing Values
val years_pre_processed = movie_fields.map( fields => fields(2)).map( x => convertYear(x)).filter( yr => (yr != 1900) ).collect()

// Equivalent Python code -->years_pre_processed_array = np.array(years_pre_processed)

/*
 # first we compute the mean and median year of release, without the 'bad' data point
 mean_year = np.mean(years_pre_processed_array[years_pre_processed_array!=1900])
 median_year = np.median(years_pre_processed_array[years_pre_processed_array!=1900])
 idx_bad_data = np.where(years_pre_processed_array==1900)[0][0]
 years_pre_processed_array[idx_bad_data] = median_year
 */

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

mean(years_pre_processed)
//Output: Int = 1989

median(years_pre_processed)
//Output: Int = 1960

//TODO - Change the python code below
//idx_bad_data = np.where(years_pre_processed_array==1900)[0][0]
//years_pre_processed_array[idx_bad_data] = median_year

//Feature Extraction

//Categorical Features: _1-of-k_ Encoding of User Occupation

val all_occupations = user_fields.map(fields=> fields(3)).distinct().collect()
scala.util.Sorting.quickSort(all_occupations)

//scala> all_occupations
//res51: Array[String] = Array(administrator, artist, doctor, educator, engineer, entertainment, executive, healthcare,
// homemaker, lawyer, librarian, marketing, none, other, programmer, retired, salesman, scientist, student, technician,
// writer)

var all_occupations_dict:Map[String, Int] = Map()
var idx = 0;
// for loop execution with a range
for( idx <- 0 to (all_occupations.length -1)){
  all_occupations_dict += all_occupations(idx) -> idx
}

/*

scala> all_occupations_dict
res4: Map[String,Int] = Map(scientist -> 17, student -> 18, writer -> 20, salesman -> 16, retired -> 15,
administrator -> 0, programmer -> 14, doctor -> 2, homemaker -> 8, executive -> 6, engineer -> 4,
 entertainment -> 5, marketing -> 11, technician -> 19, artist -> 1, librarian -> 10, lawyer -> 9,
 educator -> 3, healthcare -> 7, none -> 12, other -> 13)
*/

println("Encoding of 'doctor : " + all_occupations_dict("doctor"))
println("Encoding of 'programmer' : " + all_occupations_dict("programmer"))

/*
scala> println("Encoding of 'docter : '" + all_occupations_dict("doctor"))
Encoding of 'docter : '2

scala> println("Encoding of 'programmer' : " + all_occupations_dict("programmer"))
Encoding of 'programmer' : 14

*/

//TODO Transforming Timestamps into Categorical Features