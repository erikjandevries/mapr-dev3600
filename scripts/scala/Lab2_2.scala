val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

//Defining the Auctions case class
case class Auctions(aucid:String, bid:Float,bidtime:Float,bidder:String,bidrate:Int,openbid:Float, price:Float,itemtype:String,dtl:Int)

//Loading the data into RDD with split
val inputRDD =sc.textFile("/user/user01/data/auctiondata.csv").map(_.split(","))
// Mapping the inputRDD to the case class
val auctionsRDD = inputRDD.map(a=>Auctions(a(0),a(1).toFloat,a(2).toFloat,a(3),a(4).toInt, (5).toFloat,a(6).toFloat,a(7),a(8).toInt))

// converting auctionsRDD to a DataFrame
val auctionsDF = auctionsRDD.toDF()

//Registering the auctionsDF as a temporary table with the same name

auctionsDF.registerTempTable("auctionsDF")

//8. Check the data in the DataFrame
auctionsDF.show

//9. TO see the schema of the DataFrame
auctionsDF.printSchema

//2.2.2 - Inspect Data
//1. total number of bids
val totalbids = auctionsDF.count()

//2. Number of distinct auctions
val totalauctions = auctionsDF.select("aucid").distinct.count()

//3. Number of distinct item types
val itemtypes = auctionsDF.select("itemtype").distinct.count()

//4. Count of bids per auction and item type
auctionsDF.groupBy("itemtype","aucid").count().show()

//(you could also use take(n))


//5.For each auction item and item type, want the min, max and average number of bids
auctionsDF.groupBy("itemtype", "aucid").count.agg(min("count"), avg("count"), max("count")).show

//6. For each auction item and item type- min bid, max bid, avg bid

auctionsDF.groupBy("itemtype", "aucid").agg(min("bid"), max("bid"), avg("bid")).show

//7. Return the count of all auctions with final price greater than 200
auctionsDF.filter(auctionsDF("price")>200).count()

//8. Getting DF just for xboxes
val xboxes = sqlContext.sql("SELECT aucid,itemtype,bid,price,openbid FROM auctionsDF WHERE itemtype='xbox'")
// compute basic statistics on price across all auctions on xboxes
xboxes.describe("price").show
