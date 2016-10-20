from pyspark import SparkContext, SparkConf;
conf = SparkConf().setAppName("Lab10b");
sc = SparkContext(conf = conf);

## See also: https://spark.apache.org/docs/1.5.0/ml-decision-tree.html

from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.util import MLUtils

from pyspark.mllib.linalg import Vectors

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
flightsRDD = sc.textFile(dataFolder + "/rita2014jan.csv").map(lambda l: l.split(","))
for flight in flightsRDD.take(5):
    print flight

print "Defining data frame schema"
flightsSchema = flightsRDD.map(lambda flight: Row(
                      dofM           = flight[0]
                    , dofW           = flight[1]
                    , carrier        = flight[2]
                    , tailnum        = flight[3]
                    , flnum          = int(flight[4])
                    , org_id         = flight[5]
                    , origin         = flight[6]
                    , dest_id        = flight[7]
                    , dest           = flight[8]
                    , crsdeptime     = float(flight[9])
                    , deptime        = float(flight[10])
                    , depdelaymins   = float(flight[11])
                    , crsarrtime     = float(flight[12])
                    , arrtime        = float(flight[13])
                    , arrdelay       = float(flight[14])
                    , crselapsedtime = float(flight[15])
                    , dist           = int(flight[16])
                    , label          = (1 if float(flight[14]) > 40 else 0)
                    # , fts            = ["dofM", "dofW", "carrier", "tailnum", "flnum", "org_id", "dest_id"]
                    , fts = Vectors.dense([ float(flight[10])
                                        #     float(flight[9])
                                        #   , float(flight[10])
                                        #   , float(flight[11])
                                        #   , float(flight[12])
                                        #   , float(flight[13])
                                        #   , float(flight[14]) # this is the delay column
                                        #   , float(flight[15])
                                         ])
                    ))

# dofM = 0
# dofW = 1
# carrier = 2
# tailnum = 3
# flnum = 4
# org_id = 5
# origin = 6
# dest_id = 7
# dest = 8
# crsdeptime = 9
# deptime = 10
# depdelaymins = 11
# crsarrtime = 12
# arrtime = 13
# arrdelay = 14
# crselapsedtime = 15
# dist = 16

print "Creating data frame"
flightsDF_tmp = sqlContext.createDataFrame(flightsSchema)
# flightsDF.registerTempTable("flights")

assembler = VectorAssembler(
    inputCols=["fts"],
    outputCol="features")
flightsDF = assembler.transform(flightsDF_tmp)
# print(flightsDF.select("features", "label").first())


print flightsDF.take(1)


print "Index labels, adding metadata to the label column."
# Fit on whole dataset to include all labels in index.
labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(flightsDF)
print "Automatically identify categorical features, and index them."
# We specify maxCategories so features with > 4 distinct values are treated as continuous.
featureIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(
                    flightsDF.select([ "label"
                                     , "dofM"
                                     , "dofW"
                                     , "crsdeptime"
                                     , "crsarrtime"
                                     , "carrier"
                                     , "crselapsedtime"
                                     , "origin"
                                     , "dest"
                                     , "features"
                                    ]))



print "Split the data into training and test sets (30 percent held out for testing)"
(trainingDF, testDF) = flightsDF.randomSplit([0.7, 0.3])

print "Train a DecisionTree model."
dt = DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures")

print "Chain indexers and tree in a Pipeline"
pipeline = Pipeline(stages=[labelIndexer, featureIndexer, dt])

print "Train model.  This also runs the indexers."
model = pipeline.fit(trainingDF)

print "Make predictions."
predictions = model.transform(testDF)

print "Select example rows to display."
predictions.select("prediction", "indexedLabel", "features").show(5)

print "Select (prediction, true label) and compute test error"
evaluator = MulticlassClassificationEvaluator(
    labelCol="indexedLabel", predictionCol="prediction", metricName="precision")
accuracy = evaluator.evaluate(predictions)
print "Test Error = %g" % (1.0 - accuracy)

treeModel = model.stages[2]
print treeModel # summary only
