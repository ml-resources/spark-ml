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
