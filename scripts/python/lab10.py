from pyspark import SparkContext, SparkConf;
conf = SparkConf().setAppName("Lab10");
sc = SparkContext(conf = conf);

import os

## See also: https://www.codementor.io/spark/tutorial/building-a-recommender-with-apache-spark-python-example-app-part1

print "========================================================================"
print "Lab 10.1 - Load and Inspect data using the Spark Shell"
print "----------------------------------"
print "Loading data into Spark dataframes"
from pyspark.sql import SQLContext, Row
import pyspark.sql.functions as func
sqlContext = SQLContext(sc)

userFolder = "/user/user01"
dataFolder = userFolder + "/data"
modelsFolder = userFolder + "/models"

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
ratingsRDD.cache()
print ratingsRDD.take(5)
ratingsSchema = ratingsRDD.map(lambda rating: Row(
                      user    = int(rating[0])
                    , product = int(rating[1])
                    , rating  = float(rating[2])
                    , zip     = rating[3]
                    ))
ratingsDF = sqlContext.createDataFrame(ratingsSchema)
ratingsDF.registerTempTable("ratings")

# print "----------------------------------"
# print "Some statistics about the ratings..."
# numRatings = ratingsRDD.count()
# print "Number of ratings: ", numRatings
#
# numMovies = ratingsRDD.map(lambda x:x[1]).distinct().count()
# print "Number of movies: ", numMovies
#
# numUsers = ratingsRDD.map(lambda x:x[0]).distinct().count()
# print "Number of users: ", numUsers
#
# numZip = ratingsRDD.map(lambda x:x[3]).distinct().count()
# print "Number of zips: ", numZip

print "-----------------------------------------------------------"
print "Explore and Query the Movie Lens data with Spark DataFrames"

usersDF.printSchema()
moviesDF.printSchema()
ratingsDF.printSchema()

# # Get the max, min ratings along with the count of users who have rated a movie.
# results = sqlContext.sql("SELECT movies.title, movierates.maxr, movierates.minr, movierates.cntu from(SELECT ratings.product, max(ratings.rating) as maxr, min(ratings.rating) as minr,count(distinct user) as cntu FROM ratings group by ratings.product ) movierates join movies on movierates.product=movies.movieId order by movierates.cntu desc")
# results.show()
#
# # Show the top 10 most-active users and how many times they rated a movie
# mostActiveUsersSchemaRDD = sqlContext.sql("SELECT ratings.user, count(*) as ct from ratings group by ratings.user order by ct desc limit 10")
# mostActiveUsersSchemaRDD.show()
# # println(mostActiveUsersSchemaRDD.collect().mkString("\n"))

# Find the movies that user 4169 rated higher than 4
results = sqlContext.sql("SELECT ratings.user, ratings.product, ratings.rating, movies.title FROM ratings JOIN movies ON movies.movieId=ratings.product where ratings.user=4169 and ratings.rating > 4")
results.show()

print "========================================================================"
print "Lab 10.2 - Use Spark to Make Movie Recommendations"

print "----------------------------------"
print "Splitting dataset into training and test sets"
(trainingRatingsRDD, testRatingsRDD) = ratingsRDD.randomSplit([0.8, 0.2])
(trainingRatingsDF, testRatingsDF) = ratingsDF.randomSplit([0.8, 0.2])

numTraining = trainingRatingsRDD.count()
numTest = testRatingsRDD.count()
print "Training ratings:", numTraining
print "Test ratings:    ", numTest

print "----------------------------------"
print "Using ALS to Build a Matrix Factorization Model with the Movie Ratings data"

print("Importing Rating")
from pyspark.mllib.recommendation import Rating
print("Importing ALS")
from pyspark.mllib.recommendation import ALS

model_path = modelsFolder + "/MovieRecommendation"
if os.path.exists(model_path):
    print("Importing MatrixFactorizationModel")
    from pyspark.mllib.recommendation import MatrixFactorizationModel
    model = MatrixFactorizationModel.load(sc, model_path)
else:
    print("Training model")
    rank = 20
    numIterations = 10
    model = ALS.train(trainingRatingsRDD.map(lambda r: (r[0], r[1], r[2])) # filter User, Product, Rating
                    , rank, numIterations, 0.01)
    print("Saving model")
    model.save(sc, model_path)

print "----------------------------------"
print "Making predictions"
topRecsForUser = model.recommendProducts(4169, 10)
print topRecsForUser

movieTitles = moviesDF.map(lambda x: (x[0], x[1])).collectAsMap()
topRecommendationsForUser = [(movieTitles[r.product], r.rating) for r in topRecsForUser]
print topRecommendationsForUser

predictionsForTestRDD  = model.predictAll(testRatingsRDD.map(lambda r: (r[0], r[1])))
print predictionsForTestRDD.take(10)


print "------------------------------------------------"
print "Compare predictions with actuals for test set"


predictionsKeyedByUserProductRDD = predictionsForTestRDD.map(lambda r: ((r[0], r[1]), r[2]))
# print predictionsKeyedByUserProductRDD.take(5)
testKeyedByUserProductRDD = testRatingsRDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2])))
# print testKeyedByUserProductRDD.take(5)

testAndPredictionsJoinedRDD = testKeyedByUserProductRDD.join(predictionsKeyedByUserProductRDD)
print testAndPredictionsJoinedRDD.take(10)

print "------------------------------------------------"
print "Analyse differences"

falsePositives = testAndPredictionsJoinedRDD.filter(lambda x: ((x[1][0] <= 1) & (x[1][1] >= 4)))
  # case ((user, product), (ratingT, ratingP)) => (ratingT <= 1 && ratingP >=4)
print falsePositives.count()

sumAbsoluteError = testAndPredictionsJoinedRDD.map(lambda x: abs(x[1][0] - x[1][1])).sum()
print sumAbsoluteError

countAbsoluteError = testAndPredictionsJoinedRDD.map(lambda x: abs(x[1][0] - x[1][1])).count()
print countAbsoluteError

meanAbsoluteError = testAndPredictionsJoinedRDD.map(lambda x: abs(x[1][0] - x[1][1])).mean()
print meanAbsoluteError
