package spark.graph

import spark.RDD
import spark.SparkContext
import spark.SparkContext._
import spark.graph.impl.GraphImplWithPrimitives
import spark.graph.impl.GraphImplWithPrimitives
import spark.graph.impl.GraphImplWithPrimitives

object GraphLoader {
  
   def textFileWithVertexValues[VD: ClassManifest, ED: ClassManifest](
      sc: SparkContext,
      vertexValuesPath: String,
      edgesPath: String,
      vertexParser: (Vid, Array[String]) => VD,
      edgeParser: ((Vid, Vid), Array[String]) => ED,
      minEdgePartitions: Int = 1,
      minVertexPartitions: Int = 1)
    : Graph[VD, ED] = {

    // Parse the edge data table
    val edges = sc.textFile(edgesPath).flatMap { line =>
      if (!line.trim.isEmpty && line(0) != '#') {
        val lineArray = line.split("\\s+")
        if(lineArray.length < 2) {
          println("Invalid line: " + line)
          assert(false)
        }
        val source = lineArray(0).trim.toInt
        val target = lineArray(1).trim.toInt
        val tail = lineArray.drop(2)
        val edata = edgeParser((source, target), tail)
        println("edgeData: " + edata)
        Some(Edge(source, target, edata))
      } else {
        None
      }
    }.cache()

    println("Constructed edges...")
    println(edges.collect().deep.mkString("\n"))
    println("Printed collected edges...")

    val vertices = sc.textFile(vertexValuesPath).flatMap { line =>
      if (!line.isEmpty && line(0) != '#') {
        println("parsing line: " + line)
        val lineArray = line.split("\\s+")
        val vertexId = lineArray(0).trim().toInt
        val tail = lineArray.drop(1)
        val vdata = vertexParser(vertexId, tail)
        println("vertexId: " + vertexId + " vData: " + vdata)
        Array(Vertex(vertexId, vdata))
      } else {
        println("returning empty")
        Array.empty[Vertex[VD]]
      }
    }.cache()

    val graph = new GraphImplWithPrimitives[VD, ED](vertices, edges)
    println("Loaded graph:" +
       "\n\t#edges:    " + graph.numEdges +
       "\n\t#vertices: " + graph.numVertices)
//    println("printing edges again...")
//    println(edges.collect().deep.mkString("\n"))
//    println("finished printing edges again...")
    graph
  }
 
  /**
   * Load an edge list from file initializing the Graph RDD
   */
  def textFile[ED: ClassManifest](
      sc: SparkContext,
      path: String,
      edgeParser: Array[String] => ED,
      minEdgePartitions: Int = 1,
      minVertexPartitions: Int = 1)
    : GraphImplWithPrimitives[Int, ED] = {

    // Parse the edge data table
    val edges = sc.textFile(path).flatMap { line =>
      if (!line.isEmpty && line(0) != '#') {
        val lineArray = line.split("\\s+")
        if(lineArray.length < 2) {
          println("Invalid line: " + line)
          assert(false)
        }
        val source = lineArray(0)
        val target = lineArray(1)
        val tail = lineArray.drop(2)
        val edata = edgeParser(tail)
        println("edgeData: " + edata)
        Array(Edge(source.trim.toInt, target.trim.toInt, edata))
      } else {
        Array.empty[Edge[ED]]
      }
    }.cache()

    val graph = fromEdges(edges)
     println("Loaded graph:" +
       "\n\t#edges:    " + graph.numEdges +
       "\n\t#vertices: " + graph.numVertices)

    graph
  }

  def fromEdges[ED: ClassManifest](edges: RDD[Edge[ED]]): GraphImplWithPrimitives[Int, ED] = {
    val vertices = edges.flatMap { edge => List((edge.src, 1), (edge.dst, 1)) }
      .reduceByKey(_ + _)
      .map{ case (vid, degree) =>
        	println("vid: " + vid + " data: " + degree) 
        	Vertex(vid, degree)
        	}
    new GraphImplWithPrimitives[Int, ED](vertices, edges)
  }
}
