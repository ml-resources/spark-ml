var user_data = sc.textFile("../../data/ml-100k/u.user")

println(user_data.first())
user_data = user_data.map(l => l.replaceAll("[|]", ","))
//  1|24|M|technician|85711

val user_fields = user_data.map(l => l.split(","))

val num_users = user_fields.map(l => l(0)).count()

val num_genders = user_fields.map(l => l(2)).distinct().count()

val num_occupations = user_fields.map(l => l(3)).distinct().count()
val num_zipcodes = user_fields.map(l => l(4)).distinct().count()

//Movies Dataset


var movie_data = sc.textFile("../../data/ml-100k/u.item")
println(movie_data.first())
movie_data = movie_data.map(l => l.replaceAll("[|]", ","))
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
    case e: Exception => println("exception caught: " + e);
      return 1900
  }
}

val movie_fields = movie_data.map(lines =>  lines.split("$"))
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



