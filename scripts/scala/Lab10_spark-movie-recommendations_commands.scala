val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
import org.apache.spark.sql._
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}

case class Movie(movieId: Int, title: String)

case class User(userId: Int, gender: String, age: Int, occupation: Int, zip: String)

def parseMovie(str: String): Movie = {
  val fields = str.split("::")
  assert(fields.size == 3)
  Movie(fields(0).toInt, fields(1))
}
def parseUser(str: String): User = {
  val fields = str.split("::")
  assert(fields.size == 5)
  User(fields(0).toInt, fields(1).toString, fields(2).toInt, fields(3).toInt, fields(4).toString)
}
def parseRating(str: String): Rating = {
  val fields = str.split("::")
  Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
}

val ratingText = sc.textFile("/user/user01/data/ratings.dat")
val ratingsRDD = ratingText.map(parseRating).cache()
val numRatings = ratingsRDD.count()
val numUsers = ratingsRDD.map(_.user).distinct().count()
val numMovies = ratingsRDD.map(_.product).distinct().count()
println(s"Got $numRatings ratings from $numUsers users on $numMovies movies.")
val moviesDF= sc.textFile("/user/user01/data/movies.dat").map(parseMovie).toDF()
val usersDF = sc.textFile("/user/user01/data/users.dat").map(parseUser).toDF()
val ratingsDF = ratingsRDD.toDF()

ratingsDF.registerTempTable("ratings")
moviesDF.registerTempTable("movies")
usersDF.registerTempTable("users")

ratingsDF.select("product").distinct.count
ratingsDF.groupBy("product", "rating").count.show
ratingsDF.groupBy("product").count.agg(min("count"), avg("count"),max("count")).show
ratingsDF.select("product", "rating").groupBy("product", "rating").count.agg(min("count"), avg("count"),max("count")).show

val results =sqlContext.sql("select movies.title, movierates.maxr, movierates.minr, movierates.cntu from(SELECT ratings.product, max(ratings.rating) as maxr, min(ratings.rating) as minr,count(distinct user) as cntu FROM ratings group by ratings.product ) movierates join movies on movierates.product=movies.movieId order by movierates.cntu desc ")
val mostActiveUsersSchemaRDD = sqlContext.sql("SELECT ratings.user, count(*) as ct from ratings group by ratings.user order by ct desc limit 10")
mostActiveUsersSchemaRDD.take(20).foreach(println)

val results =sqlContext.sql("SELECT ratings.user, ratings.product, ratings.rating, movies.title FROM ratings JOIN movies ON movies.movieId=ratings.product where ratings.user=4169 and ratings.rating > 4 order by ratings.rating desc ")
val splits = ratingsRDD.randomSplit(Array(0.8, 0.2), 0L)

val trainingRatingsRDD = splits(0).cache()
val testRatingsRDD = splits(1).cache()
val numTraining = trainingRatingsRDD.count()
val numTest = testRatingsRDD.count()
println(s"Training: $numTraining, test: $numTest.")

val model = ALS.train(trainingRatingsRDD, 20, 10)
val model = (new ALS().setRank(20).setIterations(10).run(trainingRatingsRDD))

val topRecsForUser = model.recommendProducts(4169, 10)
val movieTitles=moviesDF.map(array => (array(0), array(1))).collectAsMap()
topRecsForUser.map(rating => (movieTitles(rating.product), rating.rating)).foreach(println)
val predictionsForTestRDD  = model.predict(testRatingsRDD.map{case Rating(user, product, rating) => (user, product)})
predictionsForTestRDD.take(10).mkString("\n")
val predictionsKeyedByUserProductRDD = predictionsForTestRDD.map{
  case Rating(user, product, rating) => ((user, product), rating)
}
val testKeyedByUserProductRDD = testRatingsRDD.map{
  case Rating(user, product, rating) => ((user, product), rating)
}
val testAndPredictionsJoinedRDD = testKeyedByUserProductRDD.join(predictionsKeyedByUserProductRDD)
testAndPredictionsJoinedRDD.take(10).mkString("\n")

val falsePositives =(testAndPredictionsJoinedRDD.filter{
  case ((user, product), (ratingT, ratingP)) => (ratingT <= 1 && ratingP >=4)
  })
val meanAbsoluteError = testAndPredictionsJoinedRDD.map {
  case ((user, product), (testRating, predRating)) =>
    val err = (testRating - predRating)
    Math.abs(err)
}.mean()
