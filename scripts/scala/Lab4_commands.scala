val IncidntNum = 0
val Category = 1
val Descript = 2
val DayOfWeek = 3
val Date = 4
val Time = 5
val PdDistrict = 6
val Resolution = 7
val Address = 8
val X = 9
val Y = 10
val PdId = 11

val sfpdRDD = sc.textFile("/user/user01/data/sfpd.csv").map(line=>line.split(","))
sfpdRDD.first()
sfpdRDD.take(5)
val totincs = sfpdRDD.count()
val totres = sfpdRDD.map(inc=>inc(Resolution)).distinct.count
val dists = sfpdRDD.map(inc=>inc(PdDistrict)).distinct
dists.collect
val top5Dists = sfpdRDD.map(incident=>(incident(PdDistrict),1)).reduceByKey((x,y)=>x+y).map(x=>(x._2,x._1)).sortByKey(false).take(5)
val top5Adds = sfpdRDD.map(incident=>(incident(Address),1)).reduceByKey((x,y)=>x+y).map(x=>(x._2,x._1)).sortByKey(false).take(5)
val top3Cat = sfpdRDD.map(incident=>(incident(Category),1)).reduceByKey((x,y)=>x+y).map(x=>(x._2,x._1)).sortByKey(false).take(3)
val num_inc_dist = sfpdRDD.map(incident=>(incident(PdDistrict),1)).countByKey()

val catAdd = sc.textFile("/user/user01/data/J_AddCat.csv").map(x=>x.split(",")).map(x=>(x(1),x(0)))
val distAdd = sc.textFile("/user/user01/data/J_AddDist.csv").map(x=>x.split(",")).map(x=>(x(1),x(0)))
val catJdist = catAdd.join(distAdd)
catJdist.collect
catJdist.count
catJdist.take(10)
val catJdist1 = catAdd.leftOuterJoin(distAdd)
catJdist1.collect
catJdist.count
val catJdist2 = catAdd.rightOuterJoin(distAdd)
catJdist2.collect
catJdist2.count

sfpdRDD.partitions.size
sfpdRDD.partitioner
val incByDists = sfpdRDD.map(incident=>(incident(PdDistrict),1)).reduceByKey((x,y)=>x+y)
incByDists.partitions.size
incByDists.partitioner
val inc_map = incByDists.map(x=>(x._2,x._1))
inc_map.partitions.size
inc_map.partitioner
val inc_sort = incByDists.map(x=>(x._2,x._1)).sortByKey(false)
inc_sort.partitioner
val inc_group = sfpdRDD.map(incident=>(incident(PdDistrict),1)).groupByKey()
val incByDists = sfpdRDD.map(incident=>(incident(PdDistrict),1)).reduceByKey((x,y)=>x+y,10)
incByDists.partitions.size
val catAdd = sc.textFile("/user/user01/data/J_AddCat.csv").map(x=>x.split(",")).map(x=>(x(1),x(0)))
val distAdd = sc.textFile("/user/user01/data/J_AddDist.csv").map(x=>x.split(",")).map(x=>(x(1),x(0)))
val catJdist=catAdd.join(distAdd,8)
catJdist.partitions.size
catJdist.partitioner
