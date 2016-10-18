val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
case class Auctions(aucid:String, bid:Float,bidtime:Float,bidder:String,bidrate:Int,openbid:Float, price:Float,itemtype:String,dtl:Int)
val inputRDD =sc.textFile("/user/user01/data/auctiondata.csv").map(_.split(","))
val auctionsRDD = inputRDD.map(a=>Auctions(a(0),a(1).toFloat,a(2).toFloat,a(3),a(4).toInt, (5).toFloat,a(6).toFloat,a(7),a(8).toInt))
val auctionsDF = auctionsRDD.toDF()
auctionsDF.registerTempTable("auctionsDF")
auctionsDF.show
auctionsDF.printSchema
val totalbids = auctionsDF.count()
val totalauctions = auctionsDF.select("aucid").distinct.count()
val itemtypes = auctionsDF.select("itemtype").distinct.count()
auctionsDF.groupBy("itemtype","aucid").count().show()
auctionsDF.groupBy("itemtype", "aucid").count.agg(min("count"), avg("count"), max("count")).show
auctionsDF.groupBy("itemtype", "aucid").agg(min("bid"), max("bid"), avg("bid")).show
auctionsDF.filter(auctionsDF("price")>200).count()
val xboxes = sqlContext.sql("SELECT aucid,itemtype,bid,price,openbid FROM auctionsDF WHERE itemtype='xbox'")
xboxes.describe("price").show
