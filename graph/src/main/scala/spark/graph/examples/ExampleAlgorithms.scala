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
import scala.collection.mutable.ListBuffer

object ExampleAlgorithms {

  val filterPrimitivesTestFile = "/Users/semihsalihoglu/projects/graphx/spark/test-data/test_filter_primitives.txt"
  val undirectedSmallMutlipleComponentsVerticesFileName =
    "/Users/semihsalihoglu/projects/graphx/spark/test-data/undirected_small_multiple_component_vertices.txt"
  val undirectedSmallMultipleComponentsEdgesFileName =
    "/Users/semihsalihoglu/projects/graphx/spark/test-data/undirected_small_multiple_component_edges.txt"
  val undirectedSmallSingleComponentVerticesFileName =
    "/Users/semihsalihoglu/projects/graphx/spark/test-data/undirected_small_single_component_vertices.txt"
  val undirectedSmallSingleComponentEdgesFileName =
    "/Users/semihsalihoglu/projects/graphx/spark/test-data/undirected_small_single_component_edges.txt"
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
  val randomBipartiteMatchingVerticesFileName = "/Users/semihsalihoglu/projects/graphx/spark/test-data/small_bipartite_vertices.txt"
  val randomBipartiteMatchingEdgesFileName = "/Users/semihsalihoglu/projects/graphx/spark/test-data/small_bipartite_edges.txt"    
  val semiClusteringTestVerticesFileName = "/Users/semihsalihoglu/projects/graphx/spark/test-data/semi_clustering_test_vertices.txt"
  val semiClusteringTestEdgesFileName = "/Users/semihsalihoglu/projects/graphx/spark/test-data/semi_clustering_test_edges.txt"
  val triangleFindingTestEdgesFileName = "/Users/semihsalihoglu/projects/graphx/spark/test-data/triangle_finding_test_edges.txt"

  def main(args: Array[String]) {
//    weaklyConnectedComponents()
//    singleSourceShortestPaths()
//    pageRank()
      hits("smallHitsTest", pageRankVerticesFileName, pageRankEdgesFileName)
//    stronglyConnectedComponents()
//    boruvkasMinimumSpanningTree("medium", mstMediumEWGVerticesFileName, mstMediumEWGEdgesFileName)
//      approximateMaxWeightMatching("testsmall", mwmSmallVerticesFileName, mwmSmallEdgesFileName)
//      graphColoringBasedOnMaximalIndependentSet("testsmall", undirectedSmallMutlipleComponentsVerticesFileName,
//        undirectedSmallMultipleComponentsEdgesFileName)
//	  graphColoringBasedOnMaximalIndependentSet("tinyEWG", mstTinyEWGVerticesFileName,
//        mstTinyEWGEdgesFileName)
//	  conductance("testsmall", sccVerticesFileName, sccEdgesFileName)
//    randomMaximalBipartiteMatching("smallRandomBipartiteMatching", randomBipartiteMatchingVerticesFileName,
//    randomBipartiteMatchingEdgesFileName)
//	betweennessCentrality("smallBC", directedSmallVerticesFileName, directedSmallEdgesFileName);
//	semiClustering("smallSemiClustering", semiClusteringTestVerticesFileName, semiClusteringTestEdgesFileName);
//    doubleFringeDiameterEstimation("smallDoubleFringeDiameterEstimation", undirectedSmallSingleComponentVerticesFileName, undirectedSmallSingleComponentEdgesFileName)
//    kCore("smallKCore", semiClusteringTestVerticesFileName, semiClusteringTestEdgesFileName)
//    triangleFindingPerNode("smallTriangleFindingPerNode", triangleFindingTestEdgesFileName)
//    kTruss("smallKTruss", triangleFindingTestEdgesFileName)
  }

  private def kTruss(testName: String, edgesFileName: String) = {
    // LOADING + PREPROCESSING
    val sc = new SparkContext("local", testName)
    // ALGORITHM
    val k = 4
    var edges = loadEdges(sc, edgesFileName)
    debugRDD(edges.collect, "starting edges")
    var previousNumEdges = Long.MaxValue
    var numEdges = edges.count
    var iterationNo = 1
    while (previousNumEdges != numEdges) {
      previousNumEdges = numEdges
      val triangles = findTriangles(edges)
      val numTrianglesPerEdge = triangles.flatMap(
        triangle => List((triangle._1, 1), ((triangle._1._2, triangle._1._1), 1),
          ((triangle._1._1, triangle._2._1), 1), ((triangle._2._1, triangle._1._1), 1), 
          ((triangle._1._2, triangle._2._1), 1), ((triangle._2._1, triangle._1._2), 1)))
          .reduceByKey(_ + _)
      debugRDD(numTrianglesPerEdge.collect, "num triangles per edge")
      edges = numTrianglesPerEdge.filter(edgeNumTriangles => edgeNumTriangles._2 >= (k-2))
                                 .map(edgeNumTriangles => edgeNumTriangles._1)
      debugRDD(edges.collect, "new edges for iterationNo: " + iterationNo)
      iterationNo += 1
      numEdges = edges.count
      println("previousNumEdges: " + previousNumEdges + " newNumEdges: " + numEdges)
    }
    // POSTPROCESSING
    sc.stop()
  }


  private def triangleFindingPerNode(testName: String, edgesFileName: String) = {
    // LOADING + PREPROCESSING
    val sc = new SparkContext("local", testName)
    // ALGORITHM
    val triangles = findTriangles(loadEdges(sc, edgesFileName))
    val numTrianglesPerVertex = triangles.flatMap(
      triangle => List((triangle._1._1, 1), (triangle._1._2, 1), (triangle._2._1, 1))).reduceByKey(_ + _)
    debugRDD(numTrianglesPerVertex.collect, "num triangles per vertex")
    // POSTPROCESSING
    sc.stop()
  }

  private def findTriangles(edges: RDD[(Int, Int)]): RDD[((Int, Int), (Int, Int))] = {
    val edgesAsKey = edges.map(v => (v, 1))
    val edgesGroupedByVertices = edges.groupByKey()
    val missingEdges = edgesGroupedByVertices.flatMap(vertexIDNbrIDs =>
      {
        val vertexID = vertexIDNbrIDs._1
        val nbrIDs = vertexIDNbrIDs._2
        var listOfMissingEdges: ListBuffer[((Int, Int), Int)] = ListBuffer()
        for (i <- 0 to nbrIDs.length-1) {
          if (vertexID > nbrIDs(i)) {
            for (j <- (i+1) to nbrIDs.length-1) {
              if (vertexID > nbrIDs(j)) {
            	listOfMissingEdges.add(((nbrIDs(i), nbrIDs(j)), vertexID))
              }
            }
          }
        }
        listOfMissingEdges
      })
    val triangles = missingEdges.join(edgesAsKey)
    println("num total triangles: " + triangles.count)
    triangles
  }
  
  private def loadEdges(sc: SparkContext, edgesFileName: String): RDD[(Int, Int)] = {
    sc.textFile(edgesFileName).flatMap(line =>
      if (!line.isEmpty && line(0) != '#') {
        val lineArray = line.split("\\s+")
        if (lineArray.length < 2) {
          println("Invalid line: " + line)
          assert(false)
        }
        Some((lineArray(0).trim.toInt, lineArray(1).trim.toInt))
      } else { None })
  }

  private def kCore(testName: String, vertexFileName: String, edgesFileName: String) = {
    // LOADING + PREPROCESSING
    val sc = new SparkContext("local", testName)
    var g = GraphLoader.textFileWithVertexValues(sc, vertexFileName, edgesFileName,
      (id, values) => new WCCValue(-1), (srcDstId, evalsString) => new EmptyValue())
    // ALGORITHM
    val k = 3
    val maxIterationNo = 10
    var previousNumVertices = Long.MaxValue
    var currentNumVertices = g.numVertices
    var iterationNo = 1
    while(previousNumVertices != currentNumVertices && iterationNo < maxIterationNo) {
      println("iterationNo: " + iterationNo)
      previousNumVertices = currentNumVertices
      g = g.filterVerticesUsingLocalEdges(EdgeDirection.Out,
        vIDValueAndEdges => {
          val edges = vIDValueAndEdges._2._2
          if (edges.isDefined && edges.get.size >= k) {
            true
          } else {
            false
          }
        })
      debugRDD(g.vertices.collect, "vertices after filtering for the " + iterationNo + "th time")
      currentNumVertices = g.numVertices
      iterationNo += 1
    }
    
    // POSTPROCESSING
    g = g.updateVertices(v => { v.data.wccID = v.id; v.data })
    g = g.propagateAndAggregateUntilConvergence[Int](
      EdgeDirection.Out,
      v => true /* start from all vertices */,
      v => v.wccID,
      (msg, e) => msg,
      (v, msgList) => { v.data.wccID = maxAggr(v.data.wccID, msgList); v.data})
    
    var maxComponentIDAndSize = g.vertices.map{ v => (v.data.wccID, 1) }.reduceByKey(_ + _).reduce(
      (firstComp, secondComp) => {
      if (firstComp._2 > secondComp._2) { firstComp }
      else { secondComp }
    })
    println("maxComponentID: " + maxComponentIDAndSize._1 + " componentSize: " + maxComponentIDAndSize._2)
    g = g.filterVertices(v => v.data.wccID == maxComponentIDAndSize._1)
    println("largest kCore: num vertices: " + g.numVertices + " numEdges: " + g.numEdges)
    sc.stop()
  }

  private def doubleFringeDiameterEstimation(testName: String, vertexFileName: String, edgesFileName: String) = {
    // LOADING + PREPROCESSING
    val sc = new SparkContext("local", testName)
    var g = GraphLoader.textFileWithVertexValues(sc, vertexFileName, edgesFileName,
      (id, values) => -1, (srcDstId, evalsString) => new EmptyValue())
    // ALGORITHM
    var lowerBound = Int.MinValue
    var upperBound = Int.MaxValue
    var iterationNo = 1
    val maxIterationNo = 3
    val maxVerticesToPickFromFringe = 5
    while (lowerBound < upperBound && iterationNo <= maxIterationNo) {
      val r = g.pickRandomVertex()
      println("picked r: " + r)
      val (h, lengthOfTreeFromR) = runBfsFromVertexAndCountTheLengthOfTree(g, r)
      g = h
      println("lengthOfTreeFromR: " + r + " is " + lengthOfTreeFromR)
      // Pick a random vertex from the leaf
      val a = g.pickRandomVertices(v => v.data == lengthOfTreeFromR, 1)(0)
      println("picked a: " + a)
      val (t, lengthOfTreeFromA) = runBfsFromVertexAndCountTheLengthOfTree(g, a)
      g = t
      lowerBound = scala.math.max(lowerBound, lengthOfTreeFromA)
      val halfWay = lengthOfTreeFromA / 2
      println("lowerBound: " + lowerBound + " halfway: " + halfWay)
      // pick a vertex that's in the middle
      val u = g.pickRandomVertices(v => v.data == halfWay, 1)(0)
      println("picked u: " + u)
      val (k, lengthOfTreeFromU) = runBfsFromVertexAndCountTheLengthOfTree(g, u)
      g = k
      println("lengthOfTreeFromC: " + lengthOfTreeFromU)
      val fringeOfU = g.pickRandomVertices(v => v.data == lengthOfTreeFromU, maxVerticesToPickFromFringe)
      println("fringeOfC: " + fringeOfU)
      var maxDistance = -1
      for (src <- fringeOfU) {
        println("src: " + src)
        val (tmpG, lengthOfTreeFromSrc) = runBfsFromVertexAndCountTheLengthOfTree(g, src)
        if (lengthOfTreeFromSrc > maxDistance) {
          maxDistance = lengthOfTreeFromSrc
        }
      }
      println("maxDistance: " + maxDistance + " fringOfU.size: " + fringeOfU.size)
      val twiceEccentricityMinus1 = ((2 * lengthOfTreeFromU) - 1)
      println("twiceEccentricityMinus1: " + twiceEccentricityMinus1)
      if (fringeOfU.size > 1) {
        if (maxDistance == twiceEccentricityMinus1) {
          upperBound = twiceEccentricityMinus1
          println("found the EXACT diameter: " + upperBound)
          assert(lowerBound == upperBound)
        } else if (maxDistance < twiceEccentricityMinus1) {
          upperBound = twiceEccentricityMinus1 - 1
        } else if (maxDistance == (twiceEccentricityMinus1 + 1)) {
          upperBound = lengthOfTreeFromA
          println("found the EXACT diameter with multiple fringe nodes but maxDistance matching the " +
          	"lengthOfTreeFromA. diameter: " + upperBound)
          assert(lengthOfTreeFromA == (twiceEccentricityMinus1 + 1))
          assert(lowerBound == upperBound)
        } else {
          println("ERROR maxDistance: " + maxDistance + " CANNOT be strictly > than twiceEccentricity: "
            + (twiceEccentricityMinus1 + 1))
          sys.exit(-1)
        }
      } else {
        // When there is a single vertex in the fringe of C or B(u) == twiceEccentricityMinus1
        // we find the exact diameter, which is equal to the diameter of T_u=lengthOfTreeFromA 
        upperBound = lengthOfTreeFromA
        println("found the EXACT diameter with single fringe node. diameter: " + upperBound)
        assert(lowerBound == upperBound)
      }
      upperBound = scala.math.min(upperBound, maxDistance)
      println("finished iterationNo: " + iterationNo + " lowerBound: " + lowerBound + " upperBound: " + upperBound)
      iterationNo += 1
    }    
    // POSTPROCESSING
    println("final lower bound:" + lowerBound + " final upperBound: " + upperBound)
    sc.stop()
  }

  private def runBfsFromVertexAndCountTheLengthOfTree(g: Graph[Int, EmptyValue], src: Vid): (Graph[Int, EmptyValue], Int) = {
    var h = g.updateVertices(v => if (v.id == src) { v.data = 0; v.data } else { v.data = Int.MaxValue; v.data})
    debugRDD(h.vertices.collect, "vertices after initializing distances")
    h = h.propagateAndAggregateUntilConvergence[Int](
      EdgeDirection.Out,
      v => v.id == src,
      vvals => vvals,
      (vvals, edgeValue) => vvals + 1,
      (v, msgs) => { v.data = minAggr(v.data, msgs); v.data })
    debugRDD(h.vertices.collect, "vertices after propagating from src: " + src)
    val lengthOfTreeFromSrc =  h.aggregateGlobalValueOverVertices[Int](EdgeDirection.In,
        (vAndVVals) => if (vAndVVals._2._1 < Int.MaxValue) {Some(vAndVVals._2._1)} else {None},
        (a, b) => math.max(a, b))
    (h, lengthOfTreeFromSrc)
  }
//
//  private def semiClustering(testName: String, vertexFileName: String, edgesFileName: String) = {
//    // LOADING + PREPROCESSING
//    val sc = new SparkContext("local", testName)
//    var g = GraphLoader.textFileWithVertexValues(sc, vertexFileName, edgesFileName,
//      (id, values) =>  new SemiClusteringVertexValue(Nil), (srcDstId, evalsString) => evalsString(0).toDouble)
//    // ALGORITHM
//    g = g.updateVerticesUsingLocalEdges(EdgeDirection.In,
//      (v, optionalEdges) => {
//        val weightEdges = if (optionalEdges.isDefined) sumTotalWeight(optionalEdges.get) else 0.0
//        v.data.semiClusters = List((0.0, weightEdges, List(v.id)))
//        v.data
//      })
//    debugRDD(g.vertices.collect(), "vertices after initializing semicluster values")
//    for (i <- 1 to 3) {
//    g = g.aggregateNeighborValuesFixedNumberOfIterations[List[(Double, Double, List[Vid])]](
//      EdgeDirection.Out,
//      v => true,
//      v => Some(v.data.semiClusters),
//      (vval, evals) => vval,
//      aggregateSemiClusters,
//      1 /* num iterations to run */)
//      debugRDD(g.vertices.collect(), "vertices after running one iteration of aggregation. iterationNo: " + i)
//    }
//    // POSTPROCESSING
//    val maxClusterAndScore = g.aggregateGlobalValueOverVertices[(Double, List[Vid])](
//      EdgeDirection.In, // We should give none
//      vAndVVals => {
//        val semiClusterAndScores = computeAndSortExtendedSemiClsuters(vAndVVals._2._1.semiClusters)
//        if (semiClusterAndScores.isEmpty) { None }
//        else { Some((semiClusterAndScores(0)._1, semiClusterAndScores(0)._2._3)) }
//      },
//      (a, b) => { if (a._1 > b._1) a else b })
//    println("maxClusterAndScore: " + maxClusterAndScore)
//    sc.stop()
//  }
  
  def sumTotalWeight(edges: Seq[Edge[Double]]): Double = {
    var sum = 0.0
    for (edge <- edges) { sum += edge.data }
    sum
  }

  def aggregateSemiClusters(vAndNbrs: (Vertex[SemiClusteringVertexValue], Seq[Edge[Double]]),
    nbrSemiClusterLists: Option[Seq[List[(Double, Double, List[Vid])]]]): SemiClusteringVertexValue = {
	val maxClustersToReturn = 3
    val v = vAndNbrs._1
    val nbrs = vAndNbrs._2
    var allSemiClusters = v.data.semiClusters
    println("vertex ID: " + v.id + " previousSemiClusters: " + allSemiClusters)
    if (!nbrSemiClusterLists.isDefined) {
      return new SemiClusteringVertexValue(allSemiClusters)
    }

    for (nbrSemiClusterList <- nbrSemiClusterLists.get) {
      for (nbrSemiCluster <- nbrSemiClusterList) {
        if (!nbrSemiCluster._3.contains(v.id)) {
          val weightInAndOutCluster = countWeightInAndOutCluster(nbrs, nbrSemiCluster._3)
          val weightsInCluster = nbrSemiCluster._1 + weightInAndOutCluster._1
          // The weights outside the cluster is equal to:
          // previousWeightOutCluster MINUS
          // inside weigths now in the cluster due to current vertex PLUS
          // outside weights of the current vertex
          val weightOutCluster = nbrSemiCluster._2 - weightInAndOutCluster._1 + weightInAndOutCluster._2
          println("cluster: " +  (v.id :: nbrSemiCluster._3) + " weightsInCluster: " + weightsInCluster + " weightOutCluster: " + weightOutCluster)
          allSemiClusters = (weightsInCluster, weightOutCluster, v.id :: nbrSemiCluster._3)  :: allSemiClusters
        }
      }
    }
    println("vertex ID: " + v.id + " allSemiClusters: " + allSemiClusters)
    val finalExtendedSemiClusters =computeAndSortExtendedSemiClsuters(allSemiClusters).slice(0, maxClustersToReturn)
    println("vertex ID: " + v.id + " sortedFinalExtendedSemiClusters: " + finalExtendedSemiClusters)
    var finalSemiClusters: List[(Double, Double, List[Vid])] = Nil
    for (finalExtendedSemiCluster <- finalExtendedSemiClusters) {
      finalSemiClusters = finalExtendedSemiCluster._2 :: finalSemiClusters
    }
    new SemiClusteringVertexValue(finalSemiClusters)
  }

  private def computeAndSortExtendedSemiClsuters(allSemiClusters: List[(Double, Double, List[Vid])])
    : List[(Double, (Double, Double, List[Vid]))] = {
    val boundaryEdgeScore = 1.0
    var extendedSemiClusters: List[(Double, (Double, Double, List[Vid]))] = Nil
    for (semiCluster <- allSemiClusters) {
      val numVerticesInCluster = semiCluster._3.length
      val score = (semiCluster._1 - boundaryEdgeScore*semiCluster._2) / (numVerticesInCluster * math.max(1, (numVerticesInCluster-1))/2.0)
      extendedSemiClusters = (score, semiCluster) :: extendedSemiClusters
    }
    extendedSemiClusters.sort((firstCluster, secondCluster) => firstCluster._1 > secondCluster._1)
  }

  private def countWeightInAndOutCluster(nbrs: Seq[Edge[Double]], verticesInCluster: List[Vid]): (Double, Double) = {
    println("nbrs: " + nbrs)
    println("verticesInCluster: " + verticesInCluster)
    var weightInCluster = 0.0;
    var weightOutCluster = 0.0
    for (nbr <- nbrs) {
      if (verticesInCluster.contains(nbr.dst)) { weightInCluster += nbr.data }
      else { weightOutCluster += nbr.data }
    }
    (weightInCluster, weightOutCluster)
  }

  private def betweennessCentrality(testName: String, vertexFileName: String, edgesFileName: String) = {
    // LOADING + PREPROCESSING
    val sc = new SparkContext("local", testName)
    var g = GraphLoader.textFileWithVertexValues(sc, vertexFileName, edgesFileName,
      (id, values) =>  new BetweennessCentralityVertexValue(-1, 0.0, 0.0, 0.0), (srcDstId, evalsString) => new EmptyValue)
    // ALGORITHM
    for (i <- 1 until 5) {
      val s = g.pickRandomVertex()
      g = g.updateVertices{ v => 
        if (v.id == s) {
          v.data.bfsLevel = 0; v.data.sigma = 1.0; v.data.delta = 0.0
        } else { 
          v.data.bfsLevel = Int.MaxValue; v.data.sigma = 0.0; v.data.delta = 0.0
        }
        v.data
      }
      debugRDD(g.vertices.collect(), "vertices after initializing values")
      g = g.propagateAndAggregateUntilConvergence[Int](
        EdgeDirection.Out,
        v => v.id == s,
        v => v.bfsLevel,
        (bfsLevel, edgeValue) => bfsLevel + 1,
        (v, msgs) => {v.data.bfsLevel = minAggr(v.data.bfsLevel, msgs); v.data})
      val maxBfsLevel = g.aggregateGlobalValueOverVertices[Int](EdgeDirection.In,
        (vAndVVals) => if (vAndVVals._2._1.bfsLevel < Int.MaxValue) {Some(vAndVVals._2._1.bfsLevel)} else {None},
        (a, b) => math.max(a, b))
      println ("maxBfsLevel: " + maxBfsLevel)
      debugRDD(g.vertices.collect(), "vertices before updating sigmas")
      for (j <- 1 to maxBfsLevel) {
        println("starting sigma updating level: " + j)
        g = g.propagateAndAggregateFixedNumberOfIterations[Double](
//        VerticesBasedOnNeighbors[Double](
          EdgeDirection.Out,
          v => v.data.bfsLevel == j-1,
          vvals => vvals.sigma,
          (msg, evals) => msg,
          (v, msgs) => { if (v.data.bfsLevel == j) { v.data.sigma= msgs.sum }; v.data },
          1
        )
        debugRDD(g.vertices.collect(), "vertices after updating sigmas of level " + j)
      }

      for (j <- (maxBfsLevel-1) to 0 by -1) {
        g = g.propagateAndAggregateFixedNumberOfIterations[(Double, Double)](
          EdgeDirection.In,
          v => v.data.bfsLevel == j + 1,
          vvals => (1 + vvals.delta, vvals.sigma),
          (msg, evals) => msg,
          (v, msgs) => {
            if (v.data.bfsLevel == j) {
              for (msg <- msgs) {
                v.data.delta += (v.data.sigma / msg._1) * msg._2
              }
        	  v.data.bc += v.data.delta;
            }
            v.data
           },
          1)
        debugRDD(g.vertices.collect(), "vertices after updating deltas and bc of level " + j)
      }
    }

    // POSTPROCESSING
    val maxBCVertexAndBC = g.aggregateGlobalValueOverVertices[(Int, Double)](EdgeDirection.In,
      vAndVVals => Some((vAndVVals._1, vAndVVals._2._1.bc)),
      (a, b) => if (a._2 > b._2) a else b)
    println("maxBCVertex: " + maxBCVertexAndBC._1 + " bc: " + maxBCVertexAndBC._2)
    sc.stop()
  }
  
  private def randomMaximalBipartiteMatching(testName: String, vertexFileName: String, edgesFileName: String) = {
    // LOADING + PREPROCESSING
    val sc = new SparkContext("local", testName)
    var g = GraphLoader.textFileWithVertexValues(sc, vertexFileName, edgesFileName,
      (id, values) =>  new BipartiteMatchingVertexValue(-1, -1), (srcDstId, evalsString) => new EmptyValue)
    // ALGORITHM
    var iterNo = 1
    while (g.numEdges > 0) {
      println("starting iterNo: " + iterNo)
      iterNo += 1
      g = g.updateVerticesUsingLocalEdges(
        EdgeDirection.In,
        (vertex, optionalEdges) => { 
        if ((vertex.id % 2) == 1) vertex.data
        else {
    	if (optionalEdges.isEmpty) vertex.data.actualPick = -2;
    	else {
    	  var minEdgeId = optionalEdges.get(0).src
          for (index <- 1 to (optionalEdges.get.length - 1)) {
            if (optionalEdges.get(index).src < minEdgeId) {
              minEdgeId = optionalEdges.get(index).src
            }
          }
    	  vertex.data.tmpPick = minEdgeId
    	}
    	vertex.data
      }})
      debugRDD(g.vertices.collect(), "vertices after right vertices picking a tmpPick (with min ID). iterNo: " + iterNo)
      g = g.updateAnotherVertexBasedOnSelf[Int](
        v => v.id % 2 == 0,
        v => v.data.tmpPick,
        v => v.id,
        (vvals, msgs) => {
          var minEdgeId = msgs(0)
          for (index <- 1 to (msgs.length - 1)) {
            if (msgs(index) < minEdgeId) {
              minEdgeId = msgs(index)
            }
          }
    	  vvals.actualPick = minEdgeId
    	  vvals
        })
     debugRDD(g.vertices.collect(), "vertices after left vertices pick an actualPick (with min ID). iterNo: " + iterNo)
     g = g.updateAnotherVertexBasedOnSelf[Int](
        v => v.id % 2 == 1 && v.data.actualPick >= 0,
        v => v.data.actualPick,
        v => v.id,
        (vvals, msgs) => {
          assert(1 == msgs.length, "there should be only one message if there are msgs. iterNo: " + iterNo)
    	  vvals.actualPick = msgs(0)
    	  vvals
        })
     debugRDD(g.vertices.collect(), "vertices after right vertices are notified if they're picked: iterNo: " + iterNo)
     outputMatchedVertices(g.vertices.filter(v =>  v.data.actualPick > 0), testName, iterNo)
     g = g.filterVertices(v => v.data.actualPick == -1)
    }
    // POSTPROCESSING
    sc.stop()
  }

  private def conductance(testName: String, vertexFileName: String, edgesFileName: String) = {
    // LOADING + PREPROCESSING
    val sc = new SparkContext("local", testName)
    var g = GraphLoader.textFileWithVertexValues(sc, vertexFileName, edgesFileName,
      (id, values) =>  new ConductanceValue((id % 2) == 0, 0), (srcDstId, evalsString) => new EmptyValue)

    // ALGORITHM
    val degreeOfVerticesInSubset = g.aggregateGlobalValueOverVertices[Int](EdgeDirection.Out,
      (vAndVVals) => if (vAndVVals._2._1.inSubset) {Some(vAndVVals._2._2.size)} else {None},
      (a, b) => a + b)
    println("degreeOfVerticesInSubset: " + degreeOfVerticesInSubset)
    val degreeOfVerticesNotInSubset = g.aggregateGlobalValueOverVertices[Int](EdgeDirection.Out,
      (vAndVVals) => if (!vAndVVals._2._1.inSubset) {Some(vAndVVals._2._2.size)} else {None},
      (a, b) => a + b)
    println("degreeOfVerticesNotInSubset: " + degreeOfVerticesNotInSubset)
    g = g.propagateAndAggregateFixedNumberOfIterations[Int](
      EdgeDirection.Out,
      v => true,
      vvals => if (vvals.inSubset) { 1 } else { 0 },
      (msg, evals) => msg,
      (v, msgs) => { v.data.count = msgs.sum; v.data},
      1 /* 1 iteration */)
    val edgesCrossing = g.aggregateGlobalValueOverVertices[Int](EdgeDirection.Out,
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

  private def weaklyConnectedComponents() {
    val sc = new SparkContext("local", "weaklyConnectedComponents")
    var g = GraphLoader.textFileWithVertexValues(sc, undirectedSmallMutlipleComponentsVerticesFileName,
      undirectedSmallMultipleComponentsEdgesFileName,
      (id, values) => new WCCValue(-1), (srcDstId, evalsString) => new EmptyValue()).cache()
    // ALGORITHM
    g = g.updateVertices(v => { v.data.wccID = v.id; v.data })
    g = g.propagateAndAggregateUntilConvergence[Int](
      EdgeDirection.Out,
      v => true /* start from all vertices */,
      v => v.wccID,
      (msg, e) => msg,
      (v, msgList) => { v.data.wccID = maxAggr(v.data.wccID, msgList); v.data})
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
    g = g.updateVertices(v => { v.data.distance = if (v.id == 5) 0 else Integer.MAX_VALUE; v.data })
    g = g.propagateAndAggregateUntilConvergence[Int](
// propagateForwardUntilConvergenceFromOneReflection
      EdgeDirection.Out,
      v => v.id == 5 /* start vertex */,
      vvals => vvals.distance, 
      (msg, evals) => msg + evals,
      (v, msgs) => {v.data.distance = minAggr(v.data.distance, msgs); v.data})
    // POSTPROCESSING
    val localVertices = g.vertices.collect()
    for (vertex <- localVertices) {
    	println("vertexId: " + vertex.id + " distance to vertex 5: " + vertex.data.distance)
    }
    sc.stop()
  }
  
  private def hits(testName: String, vertexFileName: String, edgesFileName: String) = {
    // LOADING + PREPROCESSING
    val sc = new SparkContext("local", testName)
    var g = GraphLoader.textFileWithVertexValues(sc, vertexFileName, edgesFileName,
      (id, values) =>  new HitsVertexValue(1.0, 1.0), (srcDstId, evalsString) => new EmptyValue)
    // ALGORITHM
    for (i <- 1 to 10) {
      g = g.aggregateNeighborValues[Double](
        EdgeDirection.Out,
        vvals => vvals.hubs,
        (v, msgs) => { v.data.authority = msgs.sum; v.data })
      val authNorm = math.sqrt(g.aggregateGlobalValueOverVertices[Double](EdgeDirection.Out /* don't need this */,
        vAndEdges => Some(vAndEdges._2._1.authority * vAndEdges._2._1.authority), _ + _)) 
      g = g.updateVertices(v => { v.data.authority /= authNorm; v.data })
      g = g.aggregateNeighborValues[Double](
        EdgeDirection.Out,
        vvals => vvals.authority,
        (v, msgs) => { v.data.hubs = msgs.sum; v.data})
      val hubsNorm = math.sqrt(g.aggregateGlobalValueOverVertices[Double](EdgeDirection.Out /* don't need this */,
        vAndEdges => Some(vAndEdges._2._1.hubs * vAndEdges._2._1.hubs), _ + _)) 
      g = g.updateVertices(v => { v.data.hubs /= hubsNorm; v.data })
    }
    // POSTPROCESSING
    val localVertices = g.vertices.collect()
    for (vertex <- localVertices) {
    	println("vertexId: " + vertex.id + " authority: " + vertex.data.authority)
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
    g = g.updateVertices(v => { v.data.pageRank = initialPageRankValue; v.data })
    for (i <- 1 to 10) {
      g = g.aggregateNeighborValues[Double](
        EdgeDirection.Out,
        vvals => vvals.pageRank,
        (v, msgs) => {
          var sum = 0.0
          for (neighborPRValue <- msgs) { sum += neighborPRValue }
          v.data.pageRank = 0.15 / numVertices + 0.85 * sum
          v.data
        })
    }
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
    var iterNo = 0
    while (g.numVertices > 0) {
      iterNo += 1
      println("iterationNo: " + iterNo + " numVertices: " + g.numVertices)
      g = g.updateVertices(v => { v.data.colorID = v.id; v.data })
      g = g.propagateAndAggregateUntilConvergence[Int](
        EdgeDirection.Out,
        v => true /* all vertices */,
        vvals => vvals.colorID,
        (msg, evals) => msg,
        (v, msgs) => {v.data.colorID = maxAggr(v.data.colorID, msgs); v.data})
        //"colorID", maxAggr
      g = g.updateVertices(v => { v.data.expectedBWIdFoundPair = (v.data.colorID, v.data.colorID == v.id); v.data })
      g = g.propagateAndAggregateUntilConvergence[(Int, Boolean)](
         EdgeDirection.In /* in transpose */,
    	 v => v.data.expectedBWIdFoundPair._2, // start from vertices that have their colorID == ID
    	 vvals => vvals.expectedBWIdFoundPair,
    	 (msg, evals) => msg,
    	  (v, msgs) => {
    	    var finalValue = v.data.expectedBWIdFoundPair
    	    if (!finalValue._2) {
    	    	for (msg <- msgs) {
    	    	  if (msg._1 == finalValue._1) {
    	    	    println("FOUND SCC! expectedBWId: " + finalValue._1) 
    	    	    finalValue = msg
    	    	  }
    	    	}
    	    }
    	    v.data.expectedBWIdFoundPair = finalValue
    	    v.data
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
    var iterNo = 0
    var numVertices = g.numVertices
    g = g.filterVerticesUsingLocalEdges(EdgeDirection.Out,
      vertexIdValueAndEdges => !vertexIdValueAndEdges._2._2.isEmpty)
    while (numVertices > 0) {
      iterNo += 1
      // Pick min weight edge
      g = g.updateVerticesUsingLocalEdges(
        EdgeDirection.Out,
        (vertex, edgesList) => {
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
      // Find the roots of conjoined trees: let v.pickedNbrID = w.ID, if w.pickedNbrID is also equal to
      // v.ID, then v and w are part of the cycle of the conjoined tree and the one with the higher ID is
      // the root.
      g = g.updateSelfUsingAnotherVertexsValue[Int](
        v => true,
        v => v.data.pickedNbrId /* where the id of the other vertex is stored */,
        otherV => otherV.data.pickedNbrId,
        (v, nbrMsg) => {
          if (nbrMsg == v.id && v.id > v.data.pickedNbrId) {
            v.data.pickedNbrId = v.id
          }
          v.data
        })
      // Find the root of the conjoined tree: updating self.pickedNbrID to self.pickedNbrID.pickedNbrID
      // until pickedNbrIDs converge.
      g = HigherLevelComputations.pointerJumping(g, v => v.data.pickedNbrId,
        (v, parentsParent) => { v.data.pickedNbrId = parentsParent; v.data })
      outputVertices(g.vertices.filter(v => v.id != v.data.pickedNbrId), testSize, iterNo)
      // form supervertices
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

      // Remove singletons
      g = g.filterVerticesUsingLocalEdges(EdgeDirection.Out, vIdDataAndEdges => !vIdDataAndEdges._2._2.isEmpty)
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
    var iterNo = 0
    var numVertices = g.numVertices
    g = g.filterVerticesUsingLocalEdges(
      EdgeDirection.Out,
      vertexIdValueAndEdges => !vertexIdValueAndEdges._2._2.isEmpty)
    while (numVertices > 0) {
      iterNo += 1
      println("starting iteration: " + iterNo)
      // Pick max weight edge
      g = g.updateVerticesUsingLocalEdges(
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
        pickedNbrId
      })
      // Check if match is successful
      g = g.updateSelfUsingAnotherVertexsValue[Int](
        v => true,
        v => v.data /* where the id is stored, the data is actually the id of other vertex */,
        otherV => otherV.data /* msg from other vertex */,
        (v, nbrMsg) => {
          if (nbrMsg != v.id) {
            println("vertexId: " + v.id + " has failed to match")
            -1 /* failed match (two nbrs have NOT picked each other). new value is -1 */ } 
          else {
            println("vertexId: " + v.id + " has succesfully matched")
            v.data  /* successful match */}})
      outputMatchedVertices(g.vertices.filter(v =>  v.data >= 0), testName, iterNo)
      // remove matched vertices
      g = g.filterVertices(v => v.data == -1)
      g = g.filterVerticesUsingLocalEdges(
        EdgeDirection.Out,
        vertexIdValueAndEdges => !vertexIdValueAndEdges._2._2.isEmpty)
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
      var iterNo = 0
      // Initialize vertices to Unknown
      g = g.updateVertices(vvals => { vvals.data.setType = Unknown; vvals.data })
      var numUnknownVertices = g.numVertices
      while (numUnknownVertices > 0) {
        println("putting randomly inset...")
        g = g.updateVerticesUsingLocalEdges(
          EdgeDirection.Out,
          (vertex, edgesList) => {
          println("running update vertices...")
          if (InSet == vertex.data.setType || NotInSet == vertex.data.setType) {
            vertex.data
          } else if (edgesList.isEmpty) {
            vertex.data.setType = InSet
          } else if (Unknown == vertex.data.setType) {
            val prob = 1.0/(2.0 * edgesList.size)
            val nextDouble =  {
              if (iterNo == 1) {
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
        g = g.propagateAndAggregateFixedNumberOfIterations[(Int, MISType)](
          EdgeDirection.Out,
          v => true,
          vvals => (vvals.vertexID, vvals.setType),
          (msg, evals) => msg,
          (v, msgs) => {
            var setType = v.data.setType
            println("selfSetType: " + setType)
            if (InSet == setType) {
              for (msg <- msgs) {
                if (InSet == msg._2 && msg._1 < v.data.vertexID) {
                  println("setting type to Unknown. msg: " + msg)
                  setType = Unknown
                }
              }
            }
            v.data.setType = setType
            v.data
          },
          1 /* one iteration */ )
        g.vertices.persist; g.edges.persist
        debugRDD(g.vertices.collect(), "vertices after resolving MIS conflicts")
        // Find whether any of the neighbors are in set g =
        g = g.propagateAndAggregateFixedNumberOfIterations[Boolean](
          EdgeDirection.Out,
          v => true,
          vvals => InSet == vvals.setType,
          (msg, evals) => msg,
          (v, msgs) => {
            var nbrIsInSet = false
            for (msg <- msgs) {
              if (msg) {
                nbrIsInSet = true
                assert(InSet != v.data.setType, "if a nbr is in set, vertex cannot be inSet")
              }
            }
            if (nbrIsInSet) v.data.setType = NotInSet;
            v.data
          },
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

  def debugRDD[A](arrayToDebug: Array[A], debugString: String) {
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

  private def outputMatchedVertices[A](verticesToOutput: RDD[Vertex[A]], testName: String, iterNo: Int) {
    println("starting outputting matched vertices...")
    val localFoundVertices = verticesToOutput.collect()
    verticesToOutput.saveAsTextFile("/Users/semihsalihoglu/projects/graphx/spark/output/mwm-results/" + testName + "" + iterNo)
    for (vertex <- localFoundVertices) {
    	println("vertexId: " + vertex.id + " matchedVertex:" + vertex.data)
    }
    println("finished outputting matched vertices...")
  }

  private def outputVertices(verticesToOutput: RDD[Vertex[MSTVertexValue]], testSize: String, iterNo: Int) = {
    println("starting outputting pickedEdges ...")
    val localFoundVertices = verticesToOutput.collect()
    verticesToOutput.saveAsTextFile("/Users/semihsalihoglu/projects/graphx/spark/output/mst-results/" + testSize + "" + iterNo)
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