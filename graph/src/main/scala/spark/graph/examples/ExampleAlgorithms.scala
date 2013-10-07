package spark.graph.examples

import scala.collection.JavaConversions._
import scala.util.Random
import spark.{ ClosureCleaner, HashPartitioner, RDD }
import spark.SparkContext
import spark.SparkContext._
import spark.graph._
import spark.graph.examples.MISType._
import spark.graph.impl.GraphImpl._
import spark.graph.impl.GraphImplWithPrimitives
import spark.graph.util.AggregationFunctions._
import spark.graph.impl.HigherLevelComputations

object ExampleAlgorithms {

  val filterPrimitivesTestFile = "/Users/semihsalihoglu/projects/graphx/spark/test-data/test_filter_primitives.txt"
  val undirectedSmallVerticesFileName =
    "/Users/semihsalihoglu/projects/graphx/spark/test-data/undirected_small_multiple_component_vertices.txt"
  val undirectedSmallEdgesFileName =
    "/Users/semihsalihoglu/projects/graphx/spark/test-data/undirected_small_multiple_component_edges.txt"
  val directedSmallIntegerWeightedVerticesFileName =
    "/Users/semihsalihoglu/projects/graphx/spark/test-data/small_integer_weighted_vertices.txt"
  val directedSmallIntegerWeightedEdgesFileName =
    "/Users/semihsalihoglu/projects/graphx/spark/test-data/small_integer_weighted_edges.txt"
  val directedSmallVerticesFileName = "/Users/semihsalihoglu/projects/graphx/spark/test-data/test_small_vertices.txt"
  val directedSmallEdgesFileName = "/Users/semihsalihoglu/projects/graphx/spark/test-data/test_small_edges.txt"
  val pageRankVerticesFileName = "/Users/semihsalihoglu/projects/graphx/spark/test-data/test_pagerank_vertices.txt"
  val pageRankEdgesFileName = "/Users/semihsalihoglu/projects/graphx/spark/test-data/test_pagerank_edges.txt"
  val sccVerticesFileName = "/Users/semihsalihoglu/projects/graphx/spark/test-data/test_scc_vertices.txt"
  val sccEdgesFileName = "/Users/semihsalihoglu/projects/graphx/spark/test-data/test_scc_edges.txt"
  val mstTestVerticesFileName = "/Users/semihsalihoglu/projects/graphx/spark/test-data/test_mst_vertices.txt"
  val mstTestEdgesFileName = "/Users/semihsalihoglu/projects/graphx/spark/test-data/test_mst_edges.txt"
  val mstTinyEWGVerticesFileName = "/Users/semihsalihoglu/projects/graphx/spark/test-data/tinyEWG_vertices.txt"
  val mstTinyEWGEdgesFileName = "/Users/semihsalihoglu/projects/graphx/spark/test-data/tinyEWG_edges.txt"
  val mstMediumEWGVerticesFileName = "/Users/semihsalihoglu/projects/graphx/spark/test-data/mediumEWG_vertices.txt"
  val mstMediumEWGEdgesFileName = "/Users/semihsalihoglu/projects/graphx/spark/test-data/mediumEWG_edges.txt"
  val mwmSmallVerticesFileName = "/Users/semihsalihoglu/projects/graphx/spark/test-data/test_mwm_small_vertices.txt"
  val mwmSmallEdgesFileName = "/Users/semihsalihoglu/projects/graphx/spark/test-data/test_mwm_small_edges.txt"

  def main(args: Array[String]) {
//    weaklyConnectedComponents()
//    singleSourceShortestPaths()
//    pageRank()
//    stronglyConnectedComponents()
//    boruvkasMinimumSpanningTree("medium", mstMediumEWGVerticesFileName, mstMediumEWGEdgesFileName)
//      approximateMaxWeightMatching("testsmall", mwmSmallVerticesFileName, mwmSmallEdgesFileName)
//      graphColoringBasedOnMaximalIndependentSet("testsmall", undirectedSmallVerticesFileName,
//        undirectedSmallEdgesFileName)
//	  graphColoringBasedOnMaximalIndependentSet("tinyEWG", mstTinyEWGVerticesFileName,
//        mstTinyEWGEdgesFileName)
	  conductance("testsmall", sccVerticesFileName, sccEdgesFileName)
  }

  private def weaklyConnectedComponents() {
    val sc = new SparkContext("local", "weaklyConnectedComponents")
    var g = GraphLoader.textFileWithVertexValues(sc, undirectedSmallVerticesFileName,
      undirectedSmallEdgesFileName,
      (id, values) => new WCCValue(-1), (srcDstId, evalsString) => new EmptyValue()).cache()
    // ALGORITHM
    g = g.mapVertices(v => { v.data.wccID = v.id; v.data })
    g = g.simplePropagateForwardFromAllReflection[Int]("wccID", maxAggr)
    // POSTPROCESSING
    var localComponentIDAndSizes = g.vertices.map{ v => (v.data.wccID, 1) }.reduceByKey(_ + _).collect()
    for (componentIDSize <- localComponentIDAndSizes) {
    	println("componentID: " + componentIDSize._1 + " size: " + componentIDSize._2)
    }
    sc.stop()
  }

  private def singleSourceShortestPaths() = {
	// LOADING + PREPROCESSING
    val sc = new SparkContext("local", "singleSourceShortestPaths")
    var g = GraphLoader.textFileWithVertexValues(sc, directedSmallIntegerWeightedVerticesFileName,
      directedSmallIntegerWeightedEdgesFileName,
      (id, values) => new SSSPValue(-1), (srcDstId, evalsString) => evalsString(0).toInt).cache()
    // ALGORITHM
    g = g.mapVertices(v => { v.data.distance = if (v.id == 5) 0 else Integer.MAX_VALUE; v.data })
    g = g.propagateForwardUntilConvergenceFromOneReflection[Int](5 /* start vertex */, "distance", 
      (vertexValue, edgeValue) => vertexValue + edgeValue, minAggr)
    // POSTPROCESSING
    val localVertices = g.vertices.collect()
    for (vertex <- localVertices) {
    	println("vertexId: " + vertex.id + " distance to vertex 5: " + vertex.data.distance)
    }
    sc.stop()
  }
  
    
  private def pageRank() = {
	// LOADING + PREPROCESSING
    val sc = new SparkContext("local", "pageRank")
    var g = GraphLoader.textFileWithVertexValues(sc, pageRankVerticesFileName, pageRankEdgesFileName,
      (id, values) => new PageRankValue(-1.0), (srcDstId, evalsString) => new EmptyValue()).cache()
    // ALGORITHM
    val numVertices = g.numVertices
    val initialPageRankValue = 1.0 / numVertices
    println("initialPageRankValue: " + initialPageRankValue)
    g = g.mapVertices(v => { v.data.pageRank = initialPageRankValue; v.data })
    g = g.simpleAggregateNeighborsFixedNumberOfIterationsReflection[Double]("pageRank", 
      (vertexPRValue, neighborsPRValues) => {
        var sum = 0.0
        for (neighborPRValue <- neighborsPRValues) { sum += neighborPRValue }
        0.15/numVertices + 0.85*sum
      },
      10 /* 10 iterations */)
    // POSTPROCESSING
    val localVertices = g.vertices.collect()
    for (vertex <- localVertices) {
    	println("vertexId: " + vertex.id + " pageRank: " + vertex.data.pageRank)
    }
    sc.stop()
  }
  
  private def stronglyConnectedComponents() = {
    // LOADING + PREPROCESSING
    val sc = new SparkContext("local", "stronglyConnectedComponents")
    var g = GraphLoader.textFileWithVertexValues(sc, sccVerticesFileName, sccEdgesFileName,
      (id, values) => new SCCValue(-1, (-1, false)), (srcDstId, evalsString) => new EmptyValue()).cache()
    // ALGORITHM
    var numIter = 0
    while (g.numVertices > 0) {
      numIter += 1
      println("iterationNo: " + numIter + " numVertices: " + g.numVertices)
      g = g.mapVertices(v => { v.data.colorID = v.id; v.data })
      g = g.simplePropagateForwardFromAllReflection[Int]("colorID", maxAggr)
      g = g.mapVertices(v => { v.data.expectedBWIdFoundPair = (v.data.colorID, v.data.colorID == v.id); v.data })
      g = g.simplePropagateInTransposeReflection[(Int, Boolean)](
    	  v => v.data.expectedBWIdFoundPair._2, // start from vertices that have their colorID == ID
    	  "expectedBWIdFoundPair",
    	  (expectedBWIdFoundPair: (Int, Boolean), msgs: Seq[(Int, Boolean)]) => {
    	    var retVal = expectedBWIdFoundPair
    	    if (!expectedBWIdFoundPair._2) {
    	    	for (msg <- msgs) {
    	    	  if (msg._1 == expectedBWIdFoundPair._1) {
    	    	    println("FOUND SCC! expectedBWId: " + expectedBWIdFoundPair._1) 
    	    	    retVal = msg
    	    	  }
    	    	}
    	    }
    	    retVal
    	  }
      )
      outputFoundVertices(g.vertices.filter(v => v.data.expectedBWIdFoundPair._2))
      g = g.filterVertices(v => !v.data.expectedBWIdFoundPair._2)
    }
    // POSTPROCESSING
    sc.stop()
  }

  private def boruvkasMinimumSpanningTree(testSize: String, vertexFileName: String, edgesFileName: String) = {
    // LOADING + PREPROCESSING
    val sc = new SparkContext("local", "boruvkasMinimumSpanningTree")
    var g = GraphLoader.textFileWithVertexValues(sc, vertexFileName, edgesFileName,
      (id, values) => new MSTVertexValue(-1, -1, -1, -1.0), (srcDstId, evalsString) => new MSTEdgeValue(evalsString(0).toDouble, srcDstId._1, srcDstId._2))

    // ALGORITHM
    var numIter = 0
    var numVertices = g.numVertices
    g = g.filterVerticesBasedOnOutgoingEdgeValues(vertexIdValueAndEdges => !vertexIdValueAndEdges._2._2.isEmpty)
    while (numVertices > 0) {
      numIter += 1
      // Pick min weight edge
      g = g.updateVerticesBasedOnOutgoingEdgeValues((vertex, edgesList) => {
        var pickedNbrId = vertex.id
        var pickedNbrOriginalSrcId, pickedNbrOriginalDstId = -1
        var pickedNbrWeight = Double.MaxValue
        if (!edgesList.isEmpty) {
          for (edge <- edgesList.get) {
            if (edge.data.weight < pickedNbrWeight || (edge.data.weight == pickedNbrWeight && edge.dst > pickedNbrId)) {
              pickedNbrWeight = edge.data.weight
              pickedNbrId = edge.dst
              pickedNbrOriginalSrcId = edge.data.originalSrcId
              pickedNbrOriginalDstId = edge.data.originalDstId
            }
          }
        }
        vertex.data.pickedNbrId = pickedNbrId
        vertex.data.pickedEdgeOriginalSrcId = pickedNbrOriginalSrcId
        vertex.data.pickedEdgeOriginalDstId = pickedNbrOriginalDstId
        vertex.data.pickedEdgeWeight = pickedNbrWeight
        vertex.data
      })
      // Find the roots of conjoined trees
      g = g.updateVertexValueBasedOnAnotherVertexsValue(vvals => vvals.data.pickedNbrId,
        (nbrValue, selfVertex) => {
          if (nbrValue.pickedNbrId == selfVertex.id && selfVertex.id > selfVertex.data.pickedNbrId) {
            selfVertex.data.pickedNbrId = selfVertex.id
          }
          selfVertex.data
        })
      // find supervertex ID
      g = HigherLevelComputations.pointerJumping(g, "pickedNbrId")
      outputVertices(g.vertices.filter(v => v.id != v.data.pickedNbrId), testSize, numIter)
      g = g.formSuperVertices(v => v.pickedNbrId,
        edges => {
          var minWeightEdge = edges(0)
          for (index <- 1 to (edges.length - 1)) {
            if (edges(index).weight < minWeightEdge.weight
              || (edges(index).weight == minWeightEdge.weight && edges(index).originalDstId > minWeightEdge.originalDstId))
              minWeightEdge = edges(index)
          }
          minWeightEdge
        },
        vertexValues => new MSTVertexValue(-1, -1, -1, -1.0),
        true /* remove self loops */ )

      // remove singletons
      g = g.filterVerticesBasedOnOutgoingEdgeValues(vIdDataAndEdges => !vIdDataAndEdges._2._2.isEmpty)
      numVertices = g.numVertices
    }
    
    // POSTPROCESSING
    sc.stop()
  }
  
  private def approximateMaxWeightMatching(testName: String, vertexFileName: String, edgesFileName: String) = {
    // LOADING + PREPROCESSING
    val sc = new SparkContext("local", "approximateMaximumWeightMatching")
    var g = GraphLoader.textFileWithVertexValues(sc, vertexFileName, edgesFileName,
      (id, values) => -1, (srcDstId, evalsString) => evalsString(0).toDouble)

    // ALGORITHM
    var numIter = 0
    var numVertices = g.numVertices
    g = g.filterVerticesBasedOnOutgoingEdgeValues(vertexIdValueAndEdges => !vertexIdValueAndEdges._2._2.isEmpty)
    while (numVertices > 0) {
      numIter += 1
      println("starting iteration: " + numIter)
      // Pick max weight edge
      g = g.updateVerticesBasedOnOutgoingEdgeValues((vertex, edgesList) => {
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
        pickedNbrId
      })
      // Check if match is successful
      g = g.updateVertexValueBasedOnAnotherVertexsValue(v => v.data,
        (nbrValue, selfVertex) => {
          if (nbrValue != selfVertex.id) {
            println("vertexId: " + selfVertex.id + " has failed to match")
            -1 /* failed match (two nbrs have NOT picked each other) */ } 
          else {
            println("vertexId: " + selfVertex.id + " has succesfully matched")
            selfVertex.data  /* succesful match */}})
      outputMatchedVertices(g.vertices.filter(v =>  v.data >= 0), testName, numIter)
      // remove matched vertices
      g = g.filterVertices(v => v.data == -1)
      g = g.filterVerticesBasedOnOutgoingEdgeValues(vertexIdValueAndEdges => !vertexIdValueAndEdges._2._2.isEmpty)
      numVertices = g.numVertices
    }
    // POSTPROCESSING
    sc.stop()
  }

  private def graphColoringBasedOnMaximalIndependentSet(testName: String, vertexFileName: String, edgesFileName: String) = {
    // LOADING + PREPROCESSING
    val sc = new SparkContext("local", "graphColoringBasedOnMaximalIndependentSet")
    var g = GraphLoader.textFileWithVertexValues(sc, vertexFileName, edgesFileName,
      (id, values) =>  new GCVertexValue(id, Unknown), (srcDstId, evalsString) => new EmptyValue)

    // ALGORITHM
    var colorID = 0
    while (g.numVertices > 0) {
      colorID += 1
      val oldEdges = g.edges
      var numIter = 0
      // Initialize vertices to Unknown
      g = g.mapVertices(vvals => { vvals.data.setType = Unknown; vvals.data })
      var numUnknownVertices = g.numVertices
      while (numUnknownVertices > 0) {
        println("putting randomly inset...")
        g = g.updateVerticesBasedOnOutgoingEdgeValues((vertex, edgesList) => {
          println("running update vertices...")
          if (InSet == vertex.data.setType || NotInSet == vertex.data.setType) {
            vertex.data
          } else if (edgesList.isEmpty) {
            vertex.data.setType = InSet
          } else if (Unknown == vertex.data.setType) {
            val prob = 1.0/(2.0 * edgesList.size)
            val nextDouble =  {
              if (numIter == 1) {
            	new Random(new Random(vertex.id).nextInt).nextDouble
              } else {
                new Random().nextDouble
              }
            }
            println("prob: " + prob + " random: " + nextDouble)
            if (nextDouble < prob) { vertex.data.setType = InSet } 
          }
          vertex.data
        })
        println("finished putting randomly inset...")
        debugRDD(g.vertices.collect(), "vertices after setting inSet or not inSet")
        // Find whether any of the vertices have also picked itself into the matching
        println("resolving inset conflicts...")
        g = g.simpleAggregateNeighborsFixedNumberOfIterations[(Int, MISType)](
          vvals => (vvals.vertexID, vvals.setType),
          (selfValue, msgs) => {
            var setType = selfValue._2
            println("selfSetType: " + setType)
            if (InSet == selfValue._2) {
              for (msg <- msgs) {
                if (InSet == msg._2 && msg._1 < selfValue._1) {
                  println("setting type to Unknown. msg: " + msg)
                  setType = Unknown
                }
              }
            }
            (selfValue._1, setType)
          },
          (vvals, finalMessage) => { vvals.setType = finalMessage._2; vvals },
          1 /* one iteration */ )
        g.vertices.persist; g.edges.persist
        debugRDD(g.vertices.collect(), "vertices after resolving MIS conflicts")
        // Find whether any of the neighbors are in set g =
        g = g.simpleAggregateNeighborsFixedNumberOfIterations[Boolean](
          vvals => InSet == vvals.setType,
          (selfValue, msgs) => {
            var nbrIsInSet = false
            for (msg <- msgs) {
              if (msg) {
                nbrIsInSet = true
                assert(selfValue == false, "if a nbr is in set, vertex cannot be inSet")
              }
            }
            nbrIsInSet
          },
          (vvals, finalMessage) => { if (finalMessage) vvals.setType = NotInSet; vvals },
          1 /* one iteration */ )
       debugRDD(g.vertices.collect(), "vertices after setting NOTINSET...")
        // Remove edges from vertices in the MIS and not in MIS
       g = g.filterEdgesBasedOnSourceDestAndValue(
          edgeTriplet => (Unknown == edgeTriplet.src.data.setType) && (Unknown == edgeTriplet.dst.data.setType))
       debugRDD(g.edges.collect(), "edges after removing edges from INSET and NOTINSET...")
       numUnknownVertices = g.vertices.filter(v => v.data.setType == Unknown).count()
      }
      outputFoundVertices(g.vertices.filter(v => InSet == v.data.setType), testName, colorID)
      val newVertices = g.vertices.filter(v => InSet != v.data.setType)
      debugRDD(newVertices.collect(), "new vertices after removing vertices from the previous MIS...")
      g = new GraphImplWithPrimitives(newVertices, oldEdges).correctEdges()
      debugRDD(g.vertices.collect(), "vertices after forming new graph...")
      debugRDD(g.edges.collect(), "edges after forming new graph...")
      numUnknownVertices = g.numVertices
    }
    // POSTPROCESSING
    sc.stop()
  }
  
  private def conductance(testName: String, vertexFileName: String, edgesFileName: String) = {
    // LOADING + PREPROCESSING
    val sc = new SparkContext("local", "conductance")
    var g = GraphLoader.textFileWithVertexValues(sc, vertexFileName, edgesFileName,
      (id, values) =>  new ConductanceValue((id % 2) == 0, 0), (srcDstId, evalsString) => new EmptyValue)

    // ALGORITHM
    val degreeOfVerticesInSubset = g.mapReduceOverVerticesUsingEdges[Int](EdgeDirection.Out,
      (vAndVVals) => if (vAndVVals._2._1.inSubset) {Some(vAndVVals._2._2.size)} else {None},
      (a, b) => a + b)
    println("degreeOfVerticesInSubset: " + degreeOfVerticesInSubset)
    val degreeOfVerticesNotInSubset = g.mapReduceOverVerticesUsingEdges[Int](EdgeDirection.Out,
      (vAndVVals) => if (!vAndVVals._2._1.inSubset) {Some(vAndVVals._2._2.size)} else {None},
      (a, b) => a + b)
    println("degreeOfVerticesNotInSubset: " + degreeOfVerticesNotInSubset)
    g = g.simpleAggregateNeighborsFixedNumberOfIterations[Int](
      vvals => if (vvals.inSubset) { 1 } else { 0 }, 
      (vertexValue, neighborMsgs) => { neighborMsgs.map(a => a).sum },
      (value, finalMessage) => { value.count = finalMessage; value },
      1 /* 10 iterations */)
    val edgesCrossing = g.mapReduceOverVerticesUsingEdges[Int](EdgeDirection.Out,
      (vAndVVals) => Some(vAndVVals._2._1.count),
      (a, b) => a + b)
    println("edgesCrossing: " + edgesCrossing)

    // POSTPROCESSING
    if (degreeOfVerticesInSubset == 0 && degreeOfVerticesNotInSubset == 0) {
      if (edgesCrossing == 0) { println("conductance is 0") }
      else { println("conductance is INFINITY") }
    } else {
      val conductance = edgesCrossing.doubleValue / scala.math.max(degreeOfVerticesInSubset, degreeOfVerticesNotInSubset).doubleValue()
      println("conductance is " + conductance)
    }
    sc.stop()
  }

  private def debugRDD[A](arrayToDebug: Array[A], debugString: String) {
    println("starting to debug " + debugString)
    println(arrayToDebug.deep.mkString("\n"))
    println("finished debugging " + debugString)
  }

  private def outputFoundVertices(verticesToOutput: RDD[Vertex[GCVertexValue]], testName: String, colorID: Int) {
    println("starting outputting colored vertices...")
    val localColoredVertices = verticesToOutput.collect()
    verticesToOutput.saveAsTextFile("/Users/semihsalihoglu/projects/graphx/spark/output/gc-results/" + testName + "" + colorID)
    for (vertex <- localColoredVertices) {
    	println("vertexId: " + vertex.id + " colorID:" + colorID)
    }
    println("finished outputting colored vertices...")
  }

  private def outputMatchedVertices(verticesToOutput: RDD[Vertex[Int]], testName: String, numIter: Int) {
    println("starting outputting matched vertices...")
    val localFoundVertices = verticesToOutput.collect()
    verticesToOutput.saveAsTextFile("/Users/semihsalihoglu/projects/graphx/spark/output/mwm-results/" + testName + "" + numIter)
    for (vertex <- localFoundVertices) {
    	println("vertexId: " + vertex.id + " matchedVertex:" + vertex.data)
    }
    println("finished outputting matched vertices...")
  }

  private def outputVertices(verticesToOutput: RDD[Vertex[MSTVertexValue]], testSize: String, numIter: Int) = {
    println("starting outputting pickedEdges ...")
    val localFoundVertices = verticesToOutput.collect()
    verticesToOutput.saveAsTextFile("/Users/semihsalihoglu/projects/graphx/spark/output/mst-results/" + testSize + "" + numIter)
    for (vertex <- localFoundVertices) {
    	println("vertexId: " + vertex.id + " pickedEdge: (" + vertex.data.pickedEdgeOriginalSrcId
    	  + ", " + vertex.data.pickedEdgeOriginalDstId + ")")
    }
    println("finished outputting colorIDs...")
  }

  private def outputExpectedBwIDs(foundVertices: RDD[Vertex[SCCValue]]) {
    println("starting outputting expected BWIDs...")
    val localFoundVertices = foundVertices.collect()
    for (vertex <- localFoundVertices) {
    	println("vertexId: " + vertex.id + " expectedBwID: " + vertex.data.expectedBWIdFoundPair)
    }
    println("finished outputting colorIDs...")
  }

  private def outputColorIDs(foundVertices: RDD[Vertex[SCCValue]]) {
    println("starting outputting colorIDs...")
    val localFoundVertices = foundVertices.collect
    for (vertex <- localFoundVertices) {
    	println("vertexId: " + vertex.id + " colorID: " + vertex.data.colorID)
    }
    println("finished outputting colorIDs...")
  }

  private def outputFoundVertices(foundVertices: RDD[Vertex[SCCValue]]) {
    println("starting outputting found vertices...")
    val localFoundVertices = foundVertices.collect
    for (vertex <- localFoundVertices) {
    	println("vertexId: " + vertex.id + " sccID: " + vertex.data.colorID)
    }
    println("finished outputting found vertices...")
  }
}