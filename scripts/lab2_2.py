# To launch pyspark shell:
# pyspark --master local[2]
#
# To run this script:
# spark-submit --master local[2] lab2_2.py

from pyspark import SparkContext, SparkConf;
conf = SparkConf().setAppName("Lab2.2");
sc = SparkContext(conf = conf);


from pyspark.sql import SQLContext, Row
import pyspark.sql.functions as func
sqlContext = SQLContext(sc)


inputRDD = sc.textFile("/user/user01/data/auctiondata.csv").map(lambda l: l.split(","))


auctions = inputRDD.map(lambda p:
                Row(  auctionid = p[0]
                    , bid       = float(p[1])
                    , bidtime   = float(p[2])
                    , bidder    = p[3]
                    , bidrate   = int(p[4])
                    , openbid   = float(p[5])
                    , price     = float(p[6])
                    , itemtype  = p[7]
                    , dtl       = int(p[8])
                    ))

# Infer the schema, and register the DataFrame as a table.
auctiondf = sqlContext.createDataFrame(auctions)
auctiondf.registerTempTable("auctions")

auctiondf.show()

auctiondf.printSchema()

totbids = auctiondf.count()
print totbids
#10654

totalauctions = auctiondf.select("auctionid").distinct().count()
print totalauctions
#627

itemtypes = auctiondf.select("itemtype").distinct().count()
print itemtypes
#3

auctiondf.groupBy("itemtype","auctionid").count().show()


auctiondf.groupBy("itemtype","auctionid").count().agg(func.min("count"), func.max("count"), func.avg("count")).show()

auctiondf.groupBy("itemtype", "auctionid").agg(func.min("bid"), func.max("bid"), func.avg("bid")).show()


expensiveitems = auctiondf.filter(auctiondf.price>200).count()
print expensiveitems
#7685L

xboxes = sqlContext.sql("SELECT auctionid, itemtype, bid, price, openbid FROM auctions WHERE itemtype = 'xbox'")
xboxes.show()

xboxes.describe("price").show()
#summary price
#count   2784
#mean    144.27594109195397
#stddev  72.93472700540288
#min     31.0
#max     501.77
