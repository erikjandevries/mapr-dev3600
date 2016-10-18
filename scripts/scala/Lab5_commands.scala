import sqlContext._
import sqlContext.implicits._
val sfpdRDD = sc.textFile("/user/user01/data/sfpd.csv").map(inc=>inc.split(","))

case class Incidents(incidentnum:String, category:String, description:String, dayofweek:String, date:String, time:String, pddistrict:String, resolution:String, address:String, X:Float, Y:Float, pdid:String)

val sfpdCase=sfpdRDD.map(inc=>Incidents(inc(0),inc(1), inc(2),inc(3),inc(4),inc(5),inc(6),inc(7),inc(8),inc(9).toFloat,inc(10).toFloat, inc(11)))
val sfpdDF=sfpdCase.toDF()
sfpdDF.registerTempTable("sfpd")

val incByDist = sfpdDF.groupBy("pddistrict").count.sort($"count".desc).show(5)
val topByDistSQL = sqlContext.sql("SELECT pddistrict, count(incidentnum) AS inccount FROM sfpd GROUP BY pddistrict ORDER BY inccount DESC LIMIT 5")
val top10Res = sfpdDF.groupBy("resolution").count.sort($"count".desc)
top10Res.show(10)
val top10ResSQL = sqlContext.sql("SELECT resolution, count(incidentnum) AS inccount FROM sfpd GROUP BY resolution ORDER BY inccount DESC LIMIT 10")
val top3Cat = sfpdDF.groupBy("category").count.sort($"count".desc).show(3)
val top3CatSQL=sqlContext.sql("SELECT category, count(incidentnum) AS inccount FROM sfpd GROUP BY category ORDER BY inccount DESC LIMIT 3")
top10ResSQL.toJSON.saveAsTextFile("/user/user01/output")

def getyear(s:String):String = {
  val year = s.substring(s.lastIndexOf('/')+1)
  year
}

sqlContext.udf.register("getyear",getyear _)
val incyearSQL=sqlContext.sql("SELECT getyear(date), count(incidentnum) AS countbyyear FROM sfpd GROUP BY getyear(date) ORDER BY countbyyear DESC")
incyearSQL.collect.foreach(println)
val inc2014 = sqlContext.sql("SELECT category,address,resolution, date FROM sfpd WHERE getyear(date)='14'")
inc2014.collect.foreach(println)
val van2015 = sqlContext.sql("SELECT category,address,resolution, date FROM sfpd WHERE getyear(date)='15' AND category='VANDALISM'")
van2015.collect.foreach(println)
van2015.count
