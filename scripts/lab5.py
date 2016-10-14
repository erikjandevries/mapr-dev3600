# To launch pyspark shell:
# pyspark --master local[2]
#
# To run this script:
# spark-submit --master local[2] lab4.py

from pyspark import SparkContext, SparkConf;
conf = SparkConf().setAppName("Lab4");
sc = SparkContext(conf = conf);

import os;
import shutil;

print "========================================================================"
print "Lab 5.1"
from pyspark.sql import SQLContext, Row
import pyspark.sql.functions as func
sqlContext = SQLContext(sc)

print "2 -------------"
#Create input RDD
sfpdRDD = sc.textFile("/user/user01/data/sfpd.csv").map(lambda inc: inc.split(","))

print "3 -------------"
# Infer the schema, and register the DataFrame as a table.
sfpdSchema=sfpdRDD.map(lambda inc: Row(
                  incidentnum = inc[0]
                , category    = inc[1]
                , description = inc[2]
                , dayofweek   = inc[3]
                , date        = inc[4]
                , time        = inc[5]
                , pddistrict  = inc[6]
                , resolution  = inc[7]
                , address     = inc[8]
                , X           = float(inc[9])
                , Y           = float(inc[10])
                , pdid        = inc[11]
                ))

sfpdDF=sqlContext.createDataFrame(sfpdSchema)

sfpdDF.registerTempTable("sfpd")

print "========================================================================"
print "Lab 5.2 Explore data in DataFrames"
print "1 -------------"
#1. Top 5 Districts
incByDist = sfpdDF.groupBy("pddistrict").count().sort(func.desc("count"))
incByDist.show(5)

topByDistSQL = sqlContext.sql("SELECT pddistrict, count(incidentnum) AS inccount FROM sfpd GROUP BY pddistrict ORDER BY inccount DESC LIMIT 5")
topByDistSQL.show()

print "2 -------------"
#2. What are the top ten resolutions?
top10Res = sfpdDF.groupBy("resolution").count().sort(func.desc("count"))
top10Res.show(10)
top10ResSQL = sqlContext.sql("SELECT resolution, count(incidentnum) AS inccount FROM sfpd GROUP BY resolution ORDER BY inccount DESC LIMIT 10")
top10ResSQL.show()

print "3 -------------"
#3. Top 3 categories
top3Cat = sfpdDF.groupBy("category").count().sort(func.desc("count"))
top3Cat.show(3)
top3CatSQL=sqlContext.sql("SELECT category, count(incidentnum) AS inccount FROM sfpd GROUP BY category ORDER BY inccount DESC LIMIT 3")
top3CatSQL.show()

print "4 -------------"
#4. Save the top 10 resolutions to a JSON file.
# with open("/user/user01/output.json", "w+") as output_file:
#     output_file.write(top10ResSQL.toJSON())
outputdir = "/user/user01/output_5.2.4.json"
if os.path.exists(outputdir):
    shutil.rmtree(outputdir)
top10ResSQL.toJSON().saveAsTextFile(outputdir)

print "========================================================================"
print "Lab 5.3 User Defined Functions"
#/5.3.1 - UDF with DataFrame operations (Scala DSL)
# Doesn't apply to PySpark

#/5.3.2 - UDF with SQL
print "1 -------------"
#/1. In PySpark, you can use registerFunction to register a Lambda function.

print "2 -------------"
#/2. register the function as a udf
sqlContext.registerFunction("getyear",lambda x:x[-2:])

print "3 -------------"
#/3. count inc by year
incyearSQL=sqlContext.sql("SELECT getyear(date), count(incidentnum) AS countbyyear FROM sfpd GROUP BY getyear(date) ORDER BY countbyyear DESC")
incyearSQL.show()

print "4 -------------"
#/4. Category, resolution and address of reported incidents in 2014
inc2014 = sqlContext.sql("SELECT category,address,resolution, date FROM sfpd WHERE getyear(date)='14'")
inc2014.show()
# Can also use collect()

print "5 -------------"
#/5. Vandalism only in 2014 with address, resolution and category
van2015 = sqlContext.sql("SELECT category,address,resolution, date FROM sfpd WHERE getyear(date)='15' AND category='VANDALISM'")
van2015.show()
van2015.count()
