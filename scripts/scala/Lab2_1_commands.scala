val auctionid = 0
val bid = 1
val bidtime = 2
val bidder = 3
val bidderrate = 4
val openbid = 5
val price = 6
val itemtype = 7
val daystolive = 8
val auctionRDD = sc.textFile("/user/user01/data/auctiondata.csv").map(_.split(","))
auctionRDD.first
auctionRDD.take(5)
val totbids = auctionRDD.count()
val totitems = auctionRDD.map(_(auctionid)).distinct().count()
val itemtypes = auctionRDD.map(_(itemtype)).distinct().count()
val bids_itemtype = auctionRDD.map(x=>(x(itemtype),1)).reduceByKey((x,y)=>x+y).collect()
val bids_auctionRDD = auctionRDD.map(x=>(x(auctionid),1)).reduceByKey((x,y)=>x+y)

import java.lang.Math
val maxbids = bids_auctionRDD.map(x=>x._2).reduce((x,y)=>Math.max(x,y))
val minbids = bids_auctionRDD.map(x=>x._2).reduce((x,y)=>Math.min(x,y))
val avgbids = totbids/totitems
