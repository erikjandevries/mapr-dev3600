from pyspark import SparkContext, SparkConf;
conf = SparkConf().setAppName("Lab5");
sc = SparkContext(conf = conf);

print "========================================================================"
print "Lab 10.1 - Load and Inspect data using the Spark Shell"
print "----------------------------------"
print "Loading data into Spark dataframes"
from pyspark.sql import SQLContext, Row
import pyspark.sql.functions as func
sqlContext = SQLContext(sc)

dataFolder = "/user/user01/data"

#Create input RDD
print "Movies"
moviesRDD = sc.textFile(dataFolder + "/movies.dat").map(lambda rating: rating.split("::"))
moviesSchema = moviesRDD.map(lambda movie: Row(
                      movieId = int(movie[0])
                    , title   = movie[1]
                    ))
moviesDF = sqlContext.createDataFrame(moviesSchema)
moviesDF.registerTempTable("movies")

print "Users"
usersRDD = sc.textFile(dataFolder + "/users.dat").map(lambda rating: rating.split("::"))
usersSchema = usersRDD.map(lambda user: Row(
                      userId     = int(user[0])
                    , gender     = user[1]
                    , age        = int(user[2])
                    , occupation = int(user[3])
                    , zip        = user[4]
                    ))
usersDF = sqlContext.createDataFrame(usersSchema)
usersDF.registerTempTable("users")

print "Ratings"
ratingsRDD = sc.textFile(dataFolder + "/ratings.dat").map(lambda rating: rating.split("::"))
# ratingsRDD.cache()
print ratingsRDD.take(5)
ratingsSchema = ratingsRDD.map(lambda rating: Row(
                      user    = int(rating[0])
                    , product = int(rating[1])
                    , rating  = float(rating[2])
                    , zip     = rating[3]
                    ))
ratingsDF = sqlContext.createDataFrame(ratingsSchema)
ratingsDF.registerTempTable("ratings")

print "----------------------------------"
print "Some statistics about the ratings..."
numRatings = ratingsRDD.count()
print "Number of ratings: ", numRatings

numMovies = ratingsRDD.map(lambda x:x[1]).distinct().count()
print "Number of movies: ", numMovies

numUsers = ratingsRDD.map(lambda x:x[0]).distinct().count()
print "Number of users: ", numUsers

numZip = ratingsRDD.map(lambda x:x[3]).distinct().count()
print "Number of zips: ", numZip

print "-----------------------------------------------------------"
print "Explore and Query the Movie Lens data with Spark DataFrames"

usersDF.printSchema()
moviesDF.printSchema()
ratingsDF.printSchema()

# Get the max, min ratings along with the count of users who have rated a movie.
results = sqlContext.sql("SELECT movies.title, movierates.maxr, movierates.minr, movierates.cntu from(SELECT ratings.product, max(ratings.rating) as maxr, min(ratings.rating) as minr,count(distinct user) as cntu FROM ratings group by ratings.product ) movierates join movies on movierates.product=movies.movieId order by movierates.cntu desc")
results.show()

# Show the top 10 most-active users and how many times they rated a movie
mostActiveUsersSchemaRDD = sqlContext.sql("SELECT ratings.user, count(*) as ct from ratings group by ratings.user order by ct desc limit 10")
mostActiveUsersSchemaRDD.show()
# println(mostActiveUsersSchemaRDD.collect().mkString("\n"))

# Find the movies that user 4169 rated higher than 4
results = sqlContext.sql("SELECT ratings.user, ratings.product, ratings.rating, movies.title FROM ratings JOIN movies ON movies.movieId=ratings.product where ratings.user=4169 and ratings.rating > 4")
results.show()

print "========================================================================"
print "Lab 10.2 - Use Spark to Make Movie Recommendations"

(trainingRatingsRDD, testRatingsRDD) = ratingsRDD.randomSplit([0.8, 0.2])
(trainingRatingsDF, testRatingsDF) = ratingsDF.randomSplit([0.8, 0.2])

numTraining = trainingRatingsRDD.count()
numTest = testRatingsRDD.count()
print "Training ratings:", numTraining
print "Test ratings:    ", numTest

print "----------------------------------"
print "Using ALS to Build a Matrix Factorization Model with the Movie Ratings data"

print "__ not yet implemented __"

# # from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
# from pyspark.ml.recommendation import ALS
# from pyspark.ml.evaluation import RegressionEvaluator
#
# # rank = 20
# # numIterations = 10
# # model = ALS.train(trainingRatingsRDD, rank, numIterations, 0.01)
#
# als = ALS(  maxIter = 5
#           , regParam = 0.01
#           , userCol = "user"
#           , itemCol = "product"
#           , ratingCol = "rating")
# model = als.fit(trainingRatingsDF)
#
# print "----------------------------------"
# # print "Making Predictions with the MatrixFactorizationModel"
# print "Making Predictions with the model"
#
# # # topRecsForUser = model.recommendProducts(4169, 5)
# # # movieTitles = moviesDF.map(lambda x: (x[0], x[1])).collectAsMap()
#
# predictions = model.transform(testRatingsDF)
# evaluator = RegressionEvaluator(  metricName="rmse"
#                                 , labelCol="rating"
#                                 , predictionCol="prediction")
# rmse = evaluator.evaluate(predictions)
# print("Root-mean-square error = " + str(rmse))
