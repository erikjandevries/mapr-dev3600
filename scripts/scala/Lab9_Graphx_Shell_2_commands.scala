import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.IntParam
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators

case class Flight(dofM:String, dofW:String, carrier:String, tailnum:String, flnum:Int, org_id:Long, origin:String, dest_id:Long, dest:String, crsdeptime:Double, deptime:Double, depdelaymins:Double, crsarrtime:Double, arrtime:Double, arrdelay:Double,crselapsedtime:Double,dist:Int)

def parseFlight(str: String): Flight = {
  val line = str.split(",")
  Flight(line(0), line(1), line(2), line(3), line(4).toInt, line(5).toLong, line(6), line(7).toLong, line(8), line(9).toDouble, line(10).toDouble, line(11).toDouble, line(12).toDouble, line(13).toDouble, line(14).toDouble, line(15).toDouble, line(16).toInt)
}

val textRDD = sc.textFile("/user/user01/data/rita2014jan.csv")
val flightsRDD = textRDD.map(parseFlight).cache()
val airports = flightsRDD.map(flight => (flight.org_id, flight.origin)).distinct
airports.take(1)
val nowhere = "nowhere"
val routes = flightsRDD.map(flight => ((flight.org_id, flight.dest_id), flight.dist)).distinct
routes.cache
routes.take(1)
val airportMap = airports.map { case ((org_id), name) => (org_id -> name) }.collect.toList.toMap
val edges = routes.map { case ((org_id, dest_id), distance) => Edge(org_id.toLong, dest_id.toLong, distance) }
edges.take(1)
val graph = Graph(airports, edges, nowhere)
val numairports = graph.numVertices
graph.vertices.take(2)
graph.edges.take(2)
graph.edges.filter { case ( Edge(org_id, dest_id,distance))=> distance > 1000}.take(3)
val numroutes = graph.numEdges
graph.triplets.take(3).foreach(println)
graph.triplets.sortBy(_.attr, ascending=false).map(triplet =>
         "Distance " + triplet.attr.toString + " from " + triplet.srcAttr + " to " + triplet.dstAttr + ".").take(10).foreach(println)

def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
  if (a._2 > b._2) a else b
}

val maxInDegree: (VertexId, Int)  = graph.inDegrees.reduce(max)
val maxOutDegree: (VertexId, Int) = graph.outDegrees.reduce(max)
val maxDegrees: (VertexId, Int)   = graph.degrees.reduce(max)
airportMap(10397)
graph.inDegrees.collect.sortWith(_._2 > _._2).map(x => (airportMap(x._1), x._2))
val maxIncoming = graph.inDegrees.collect.sortWith(_._2 > _._2).map(x => (airportMap(x._1), x._2)).take(3)
maxIncoming.foreach(println)

graph.outDegrees.join(airports).sortBy(_._2._1, ascending=false).take(1)
val maxout= graph.outDegrees.join(airports).sortBy(_._2._1, ascending=false).take(3)
maxout.foreach(println)

val maxOutgoing = graph.outDegrees.collect.sortWith(_._2 > _._2).map(x => (airportMap(x._1), x._2)).take(3)
maxOutgoing.foreach(println)

val ranks = graph.pageRank(0.1).vertices
val impAirports = ranks.join(airports).sortBy(_._2._1, false).map(_._2._2)
impAirports.take(4)
graph.edges.filter { case ( Edge(org_id, dest_id,distance))=> distance > 1000}.take(3)
val sourceId: VertexId = 13024
val gg = graph.mapEdges(e => 50.toDouble + e.attr.toDouble/20  )
val initialGraph = gg.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)

val sssp = initialGraph.pregel(Double.PositiveInfinity)(
  (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
  triplet => {  // Send Message
    if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
      Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
    } else {
      Iterator.empty
    }
  },
  (a,b) => math.min(a,b) // Merge Message
)
println(sssp.vertices.take(4).mkString("\n"))

println(sssp.edges.take(4).mkString("\n"))

sssp.edges.map{ case ( Edge(org_id, dest_id,price))=> ( (airportMap(org_id), airportMap(dest_id), price)) }.top(4)(Ordering.by(_._3))
sssp.edges.map{ case ( Edge(org_id, dest_id,price))=> ( (airportMap(org_id), airportMap(dest_id), price)) }.takeOrdered(10)(Ordering.by(_._3))
sssp.vertices.collect.map(x => (airportMap(x._1), x._2)).sortWith(_._2 < _._2)
