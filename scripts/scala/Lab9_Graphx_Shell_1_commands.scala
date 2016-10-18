import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

val vertices=Array((1L, ("SFO")),(2L, ("ORD")),(3L,("DFW")))
val vRDD= sc.parallelize(vertices)
val edges = Array(Edge(1L,2L,1800),Edge(2L,3L,800),Edge(3L,1L,1400))
val eRDD= sc.parallelize(edges)
val nowhere = ("nowhere")
val graph = Graph(vRDD,eRDD, nowhere)

graph.vertices.collect.foreach(println)
graph.edges.collect.foreach(println)
graph.triplets.collect.foreach(println)
println(graph.inDegrees)

val numairports = graph.numVertices
val numroutes = graph.numEdges

graph.edges.filter { case Edge(src, dst, prop) => prop > 1000 }.count
graph.edges.filter { case Edge(src, dst, prop) => prop > 1000 }.collect.foreach(println)
graph.triplets.sortBy(_.attr, ascending=false).map(triplet =>
         "Distance " + triplet.attr.toString + " from " + triplet.srcAttr + " to " + triplet.dstAttr + ".").collect.foreach(println)
val ranks = graph.pageRank(0.1).vertices
ranks.take(3)
val impAirports = ranks.join(vRDD).sortBy(_._2._1, false).map(_._2._2)

impAirports.collect.foreach(println)
