package spark.graph.examples

import spark.SparkContext
import spark.SparkContext._
import spark.{ ClosureCleaner, HashPartitioner, RDD }
import spark.graph.GraphLoader
import spark.graph.Graph
import scala.collection.mutable.ListBuffer
import spark.graph.impl.HigherLevelComputations
import spark.graph.Edge
import spark.graph.Vertex
import spark.graph.impl.GraphImplWithPrimitives
import spark.graph.EdgeDirection

object SimpleMetis {
  val undirectedMetisTestVerticesFileName =
    "/Users/semihsalihoglu/projects/graphx/spark/test-data/metis_test_vertices.txt"
  val undirectedMetisTestEdgesFileName =
    "/Users/semihsalihoglu/projects/graphx/spark/test-data/metis_test_edges.txt"

  def main(args: Array[String]) {
    simpleMETIS("simpleMETIS", undirectedMetisTestVerticesFileName, undirectedMetisTestEdgesFileName)
  }

  private def simpleMETIS(testName: String, vertexFileName: String, edgesFileName: String) = {
    // LOADING + PREPROCESSING
    val sc = new SparkContext("local", testName)
    var g = GraphLoader.textFileWithVertexValues(sc, vertexFileName, edgesFileName,
      (id, values) => new MetisVertexValue(1.0, -1, -1, -1), (srcDstId, evalsString) => evalsString(0).toDouble)
    val numPartitions = 2
    val thresholdNumEdgesForInitialPartitioning = 3
    // ALGORITHM
    // Step 1: Coarsening
    var coarsenedGraphs: ListBuffer[Graph[MetisVertexValue, Double]] = new ListBuffer()
    while (g.numEdges > thresholdNumEdgesForInitialPartitioning) {
      val graphWithSuperVertexIDsAndCoarsenedGraph = coarsenGraph(g)
      coarsenedGraphs.append(graphWithSuperVertexIDsAndCoarsenedGraph._1)
      println("coarsened graph. numEdges: " + graphWithSuperVertexIDsAndCoarsenedGraph._2.numEdges)
      g = graphWithSuperVertexIDsAndCoarsenedGraph._2
    }
    
    // Step 2: Initial Partitioning
    var superVertexIDPartitionID = doInitialPartitioning(sc, g, numPartitions) 
    ExampleAlgorithms.debugRDD(superVertexIDPartitionID.collect, "initial partitioning results")
    // Step 3: Uncoarsen and refine
    var finalGraph: Graph[MetisVertexValue, Double] = null
    for (i <- coarsenedGraphs.length - 1 to 0 by -1) {
      println("i: " + i)
      val coarsenedGraph = coarsenedGraphs(i)
      ExampleAlgorithms.debugRDD(coarsenedGraph.vertices.collect, "coarsened graph vertices i: " + i)
      val superVertexIDVertexRDD = coarsenedGraph.vertices.map(v => (v.data.superVertexID, v))
      val superVertexIDVertexPartitionIDRDD = superVertexIDVertexRDD.join(superVertexIDPartitionID)
      val newVertices = superVertexIDVertexPartitionIDRDD.map{
        superVertexIDVertexPartitionID => {
          superVertexIDVertexPartitionID._2._1.data.partitionID = superVertexIDVertexPartitionID._2._2
          superVertexIDVertexPartitionID._2._1
        }
      }
      ExampleAlgorithms.debugRDD(newVertices.collect, "new vertices iteration: " + i)
      var uncoarsenedGraph: Graph[MetisVertexValue, Double] = new GraphImplWithPrimitives(newVertices,
        coarsenedGraph.edges)
      uncoarsenedGraph = refineUncoarsenedGraph(uncoarsenedGraph)
      superVertexIDPartitionID = uncoarsenedGraph.vertices.map{ v => (v.id, v.data.partitionID) }
      if (i == 0) {
        finalGraph = uncoarsenedGraph
      }
    }

    ExampleAlgorithms.debugRDD(finalGraph.vertices.collect, "final vertices")
    ExampleAlgorithms.debugRDD(finalGraph.edges.collect, "final edges")

    // POSTPROCESSING
    val totalWeightAcrossPartitions = finalGraph.triplets.map(e => if (e.src.data.partitionID != e.dst.data.partitionID) { e.data } else { 0.0 })
       .reduce(_ + _)
    println("totalWeightOfEdgesCrossingPartitions: " + totalWeightAcrossPartitions)
    sc.stop()
  }
  
  private def refineUncoarsenedGraph(g: Graph[MetisVertexValue, Double]): Graph[MetisVertexValue, Double] = {
    // TODO(semih): Implement
    g
  }
  
  private def doInitialPartitioning(sc: SparkContext, g: Graph[MetisVertexValue, Double],
    numPartitions: Int): RDD[(Int, Int)] = {
	// TODO(semih): There needs to be a sequential algorithm ran here! This is for demonstration
    // purposes for metis_test_vertices/edges.txt files.
    val tmpSupervertexIDPartitionID: Array[(Int, Int)] = Array((3, 1), (7, 2))
    sc.parallelize(tmpSupervertexIDPartitionID, 4)
  }
  
  private def coarsenGraph(g: Graph[MetisVertexValue, Double]):
  (Graph[MetisVertexValue, Double], Graph[MetisVertexValue, Double]) = {
      var h = g.updateVerticesUsingLocalEdges(
        EdgeDirection.Out,
        (vertex, edgesList) => {
        var pickedNbrId = vertex.id
        var pickedNbrWeight = Double.MinValue
        if (!edgesList.isEmpty) {
          for (edge <- edgesList.get) {
            if (edge.data > pickedNbrWeight || (edge.data == pickedNbrWeight && edge.dst > pickedNbrId)) {
              pickedNbrWeight = edge.data
              pickedNbrId = edge.dst
            }
          }
        }
        vertex.data.superVertexID = pickedNbrId
        vertex.data
      })
      // Find the roots of conjoined trees
      h = h.updateSelfUsingAnotherVertexsValue[Int](
        v => true,
        v => v.data.superVertexID,
        otherV => otherV.data.superVertexID,
        (v, msg) => {
          if (msg == v.id && v.id > v.data.superVertexID) {
            v.data.superVertexID = v.id
          }
          v.data
        })
      // find supervertex ID
      h = HigherLevelComputations.pointerJumping(h, v => v.data.superVertexID,
        { (v, parentsParent) => v.data.superVertexID = parentsParent; v.data })
      val coarsenedGraph = h.formSuperVertices(v => v.superVertexID,
        edges => {
          var totalWeight = edges(0)
          for (index <- 1 to (edges.length - 1)) {
        	  totalWeight += edges(index)
          }
          totalWeight
        },
        vertexValues => {
          val superVertexID = vertexValues(0).superVertexID
          var totalWeight = vertexValues(0).weight
          for (index <- 1 to (vertexValues.length - 1)) {
        	  totalWeight += vertexValues(index).weight
          }
          new MetisVertexValue(totalWeight, superVertexID, -1, -1)
        },
        true /* remove self loops */ )
      (h, coarsenedGraph)
  }
}