package spark.graph.impl

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import spark._
import spark.SparkContext._
import spark.bagel.Bagel
import spark.bagel.examples._
import spark.graph._
import spark.graph.impl._
import spark.graph.examples.DoubleIntInt
import spark.graph.util.AggregationFunctions._

@RunWith(classOf[JUnitRunner])
class GraphImplWithPrimitivesTest extends FunSuite with Serializable {
  var numVPart = 4
  var numEPart = 4
  val host = "local"
  val filterPrimitivesTestFile = "/Users/semihsalihoglu/projects/graphx/spark/test-data/test_filter_primitives.txt"
  val testSmallGraphVertexFile = "/Users/semihsalihoglu/projects/graphx/spark/test-data/test_small_vertices.txt"
  val testSmallGraphEdgeFile = "/Users/semihsalihoglu/projects/graphx/spark/test-data/test_small_edges.txt"
  val testSmallNotConnectedGraphVertexFile = "/Users/semihsalihoglu/projects/graphx/spark/test-data/test_small_notconnected_vertices.txt"
  val testSmallNotConnectedGraphEdgeFile = "/Users/semihsalihoglu/projects/graphx/spark/test-data/test_small_notconnected_edges.txt"
  val formSuperverticesTestVerticesFileName = "/Users/semihsalihoglu/projects/graphx/spark/test-data/test_form_supervertices_vertices.txt"
  val formSuperverticesTestEdgesFileName = "/Users/semihsalihoglu/projects/graphx/spark/test-data/test_form_supervertices_edges.txt"
  val smallIntegerWeightedGraphVerticesFileName = "/Users/semihsalihoglu/projects/graphx/spark/test-data/small_integer_weighted_vertices.txt"
  val smallIntegerWeightedGraphEdgesFileName = "/Users/semihsalihoglu/projects/graphx/spark/test-data/small_integer_weighted_edges.txt"
  val updateAnotherVertexTestVerticesFileName = "/Users/semihsalihoglu/projects/graphx/spark/test-data/update_another_vertex_test_vertices.txt"
  val updateAnotherVertexTestEdgesFileName = "/Users/semihsalihoglu/projects/graphx/spark/test-data/update_another_vertex_test_edges.txt"    

  test("filterEdgesTest") {
    var (sc, g) = loadGraphAndCache(filterPrimitivesTestFile, "filterEdgesTest")
    System.out.println("Before: numVertices: " + g.numVertices + " numEdges: " + g.numEdges);
    println("filtering edges by edge.data > 0.5")
    g = g.filterEdges(e => e.data > 0.5)
    assertNumVerticesAndEdgesAndStop(sc, g, 5 /* expected num vertices */ , 4 /* expected num edges */ )
    sc.stop()
  }

  test("filterVerticesTest") {
    var (sc, g) = loadGraphAndCache(filterPrimitivesTestFile, "filterVerticesTest")
    System.out.println("Before: numVertices: " + g.numVertices + " numEdges: " + g.numEdges);
    println("filtering vertices with id <= 2")
    g = g.filterVertices(v => v.id <= 2)
    assertNumVerticesAndEdgesAndStop(sc, g, 3 /* expected num vertices */ , 5 /* expected num edges */ )
  }

  test("filterEdgesBasedOnSourceDestAndValueTest") {
    var (sc, g) = loadGraphAndCache(filterPrimitivesTestFile, "filterEdgesBasedOnSourceDestAndValueTest")
    System.out.println("Before: numVertices: " + g.numVertices + " numEdges: " + g.numEdges);
    g = g.filterEdgesBasedOnSourceDestAndValue(edgeTriplet => edgeTriplet.dst.data <= 3
      && edgeTriplet.src.data <= 3)
    assertNumVerticesAndEdgesAndStop(sc, g, 5 /* expected num vertices */ , 2 /* expected num edges */ )
  }

  test("filterVerticesBasedOnOutNeighborValuesTest") {
    var (sc, g) = loadGraphAndCache(filterPrimitivesTestFile, "filterVerticesBasedOnOutNeighborValuesTest")
    System.out.println("Before: numVertices: " + g.numVertices + " numEdges: " + g.numEdges);
    // Filter vertices that have an outgoing edge with a value > 0.7
    g = g.filterVerticesUsingLocalEdges(EdgeDirection.Out,
      vIDVDataAndNeighbors => {
        val optionalListOfEdges = vIDVDataAndNeighbors._2._2
        if (optionalListOfEdges.isEmpty) true
        else {
          val listOfEdges = optionalListOfEdges.get
          listOfEdges.filter(e => e.data > 0.7).isEmpty
        }
      })
    assertNumVerticesAndEdgesAndStop(sc, g, 3, 1)
  }

  test("filterVerticesBasedOnInNeighborValuesTest") {
    var (sc, g) = loadGraphAndCache(filterPrimitivesTestFile, "filterVerticesBasedOnInNeighborValuesTest")
    System.out.println("Before: numVertices: " + g.numVertices + " numEdges: " + g.numEdges);
    // Filter vertices that have an incoming edge with a value > 0.7
    g = g.filterVerticesUsingLocalEdges(EdgeDirection.In,
      vIDVDataAndNeighbors => {
        val optionalListOfEdges = vIDVDataAndNeighbors._2._2
        if (optionalListOfEdges.isEmpty) true
        else {
          val listOfEdges = optionalListOfEdges.get
          listOfEdges.filter(e => e.data > 0.7).isEmpty
        }
      })
    assertNumVerticesAndEdgesAndStop(sc, g, 3, 2)
  }

  test("filterVerticesBasedOnBothInAndOutNeighborValuesTest") {
    var (sc, g) = loadGraphAndCache(filterPrimitivesTestFile,
      "filterVerticesBasedOnBothInAndOutNeighborValuesTest")
    System.out.println("Before: numVertices: " + g.numVertices + " numEdges: " + g.numEdges);
    // Filter vertices that have an incoming edge with a value == 0.6 (3 and 4 should be thrown away)
    g = g.filterVerticesUsingLocalEdges(EdgeDirection.Both,
      vIDVDataAndNeighbors => {
        val optionalListOfEdges = vIDVDataAndNeighbors._2._2
        if (optionalListOfEdges.isEmpty) true
        else {
          val listOfEdges = optionalListOfEdges.get
          listOfEdges.filter(e => e.data == 0.6).isEmpty
        }
      })
    assertNumVerticesAndEdgesAndStop(sc, g, 3, 5)
  }

  test("updateVertexValueBasedOnAnotherVertexsValueTest") {
    var (sc, g) = loadGraphAndCache(filterPrimitivesTestFile,
      "updateVertexValueBasedOnAnotherVertexsValueTest")
    System.out.println("Before: numVertices: " + g.numVertices + " numEdges: " + g.numEdges);
    // We shift the values of each vertex i to the value of (i-1)%5. That is, vertex 1 gets vertex 0's value
    // vertex 2 gets vertex 1's value, ..., and vertex 0 gets vertex 4's value. 
    g = g.updateSelfUsingAnotherVertexsValue[Int](
      v => true,
      v => (v.id - 1 + 5) % 5 /* id function */,
      otherV => otherV.data,
      (v, msg) => { v.data = msg; v.data })

    val localVertices = g.vertices.collect()
    for (vertex <- localVertices) {
      System.out.println("vertex.id: " + vertex.id + " data: " + vertex.data)
      if (vertex.id == 0) {
        assert(1 == vertex.data) // previously vertex 0 used to have value 5
      } else if (vertex.id == 1) {
        assert(5 == vertex.data) // previously vertex 1 used to have value 3
      } else if (vertex.id == 2) {
        assert(3 == vertex.data) // previously vertex 2 used to have value 3
      } else if (vertex.id == 3) {
        assert(3 == vertex.data) // previously vertex 3 used to have value 2
      } else if (vertex.id == 4) {
        assert(2 == vertex.data) // previously vertex 4 used to have value 1
      }
    }
    sc.stop()
  }

  test("updateAnotherVertexBasedOnSelfTest") {
    var (sc, g) = loadGraphFromVertexAndEdgeFileAndCache(updateAnotherVertexTestVerticesFileName,
      updateAnotherVertexTestEdgesFileName, "updateAnotherVertexBasedOnSelfTest")
    g = g.updateAnotherVertexBasedOnSelf[Double](v => v.id < 14 && v.data.intValue1 > 0,
      v => v.data.intValue1, v => v.data.doubleValue,
      (vvals, msgs) => { val sum = msgs.sum; vvals.doubleValue = sum; vvals })

    assert(0 == g.numEdges)
    assert(15 == g.numVertices)
    val localVertices = g.vertices.collect()
    for (vertex <- localVertices) {
      assert(-1 == vertex.data.intValue2)
      if (vertex.id == 1) {
        assert(0.1 == getDoubleValueRoundedToTwoDigits(vertex.data.doubleValue)); assert(2 == vertex.data.intValue1)
      } else if (vertex.id == 2) {
        assert(0.1 == getDoubleValueRoundedToTwoDigits(vertex.data.doubleValue)); assert(-1 == vertex.data.intValue1)
      } else if (vertex.id == 3) {
        assert(0.3 == getDoubleValueRoundedToTwoDigits(vertex.data.doubleValue)); assert(4 == vertex.data.intValue1)
      } else if (vertex.id == 4) {
        assert(0.8 == getDoubleValueRoundedToTwoDigits(vertex.data.doubleValue)); assert(-1 == vertex.data.intValue1)
      } else if (vertex.id == 5) {
        assert(0.5 == getDoubleValueRoundedToTwoDigits(vertex.data.doubleValue)); assert(4 == vertex.data.intValue1)
      } else if (vertex.id == 6) {
        assert(0.7 == getDoubleValueRoundedToTwoDigits(vertex.data.doubleValue)); assert(7 == vertex.data.intValue1)
      } else if (vertex.id == 7) {
        assert(0.6 == getDoubleValueRoundedToTwoDigits(vertex.data.doubleValue)); assert(6 == vertex.data.intValue1)
      } else if (vertex.id == 8) {
        assert(3.3 == getDoubleValueRoundedToTwoDigits(vertex.data.doubleValue)); assert(-1 == vertex.data.intValue1)
      } else if (vertex.id == 9) {
        assert(0.9 == getDoubleValueRoundedToTwoDigits(vertex.data.doubleValue)); assert(8 == vertex.data.intValue1)
      } else if (vertex.id == 10) {
        assert(1.0 == getDoubleValueRoundedToTwoDigits(vertex.data.doubleValue)); assert(-1 == vertex.data.intValue1)
      } else if (vertex.id == 11) {
        assert(1.1 == getDoubleValueRoundedToTwoDigits(vertex.data.doubleValue)); assert(8 == vertex.data.intValue1)
      } else if (vertex.id == 12) {
        assert(1.2 == getDoubleValueRoundedToTwoDigits(vertex.data.doubleValue)); assert(-1 == vertex.data.intValue1)
      } else if (vertex.id == 13) {
        assert(1.3 == getDoubleValueRoundedToTwoDigits(vertex.data.doubleValue)); assert(8 == vertex.data.intValue1)
      } else if (vertex.id == 14) {
        assert(1.4 == getDoubleValueRoundedToTwoDigits(vertex.data.doubleValue)); assert(-1 == vertex.data.intValue1)
      } else if (vertex.id == 15) {
        assert(1.5 == getDoubleValueRoundedToTwoDigits(vertex.data.doubleValue)); assert(8 == vertex.data.intValue1)
      }
    }
    sc.stop()
  }

  test("pickRandomVertexTest") {
    var (sc, g) = loadGraphAndCache("/Users/semihsalihoglu/projects/graphx/spark/test-data/test_pick_random_vertex.txt",
      "pickRandomVertexTest")
    val vertexId = g.pickRandomVertex()
    println("picked vertex id: " + vertexId)
    assert(vertexId >= 100 && vertexId <= 1000)
    sc.stop()
  }

  test("pickRandomVerticesAmongstTest") {
    var (sc, g) = loadGraphAndCache("/Users/semihsalihoglu/projects/graphx/spark/test-data/test_pick_random_vertex.txt",
      "pickRandomVertexTest")
    val verticesPicked = g.pickRandomVertices(v => v.id > 500, 2)
    println("picked vertices: " + verticesPicked)
    assert(2 == verticesPicked.size)
    assert(verticesPicked(0) > 500 && verticesPicked(0) <= 1000)
    assert(verticesPicked(1) > 500 && verticesPicked(1) <= 1000)
    sc.stop()
  }
  test("propagateForwardFixedNumberOfIterationsOneIterationTest") {
    var (sc, g) = loadGraphAndRunPropagateFixNumberOfIterations(1, EdgeDirection.Out,
      "propagateForwardFixedNumberOfIterationsOneIterationTest")
	collectVerticesAndVerifyOneIterationOfDoubleValuePropagationForwardAndStopSparkContext(sc, g)  
  }

  private def collectVerticesAndVerifyOneIterationOfDoubleValuePropagationForwardAndStopSparkContext(
    sc: SparkContext,
    g: Graph[DoubleIntInt, Double]) = {
    val localVertices = g.vertices.collect()
    for (vertex <- localVertices) {
      checkIntValue1AndIntValue2AreUnchanged(vertex)
      val doubleValue = getDoubleValueRoundedToTwoDigits(vertex.data.doubleValue)
      if (vertex.id == 0) {
        assert(0.0 == doubleValue)
      } else if (vertex.id == 1) {
        assert(0.05 == doubleValue)
      } else if (vertex.id == 2) {
        assert(0.2 == doubleValue)
      } else if (vertex.id == 3) {
        assert(0.3 == doubleValue)
      } else if (vertex.id == 4) {
        assert(0.4 == doubleValue)
      } else if (vertex.id == 5) {
        assert(0.5 == doubleValue)
      } else if (vertex.id == 6) {
        assert(0.6 == doubleValue)
      } else if (vertex.id == 7) {
        assert(0.7 == doubleValue)
      }
    }
    sc.stop()
  }

  test("propagateInTransposeFixedNumberOfIterationsOneIterationTest") {
    var (sc, g) = loadGraphAndRunPropagateFixNumberOfIterations(1, EdgeDirection.In,
      "propagateInTransposeFixedNumberOfIterationsOneIterationTest")
    collectVerticesAndVerifyOneIterationOfDoubleValuePropagationInTransposeAndStopSparkContext(sc, g)
  }
//  
//  test("propagateInTransposeFixedNumberOfIterationsReflectionOneIterationTest") {
//    var (sc, g) = loadGraphAndRunPropagateFixNumberOfIterationsReflection(1, EdgeDirection.In,
//      "propagateInTransposeFixedNumberOfIterationsReflectionOneIterationTest")
//    collectVerticesAndVerifyOneIterationOfDoubleValuePropagationInTransposeAndStopSparkContext(sc, g)
//  }

  private def collectVerticesAndVerifyOneIterationOfDoubleValuePropagationInTransposeAndStopSparkContext(
    sc: SparkContext,
    g: Graph[DoubleIntInt, Double]) = {
    val localVertices = g.vertices.collect()
    for (vertex <- localVertices) {
      checkIntValue1AndIntValue2AreUnchanged(vertex)
      val doubleValue = getDoubleValueRoundedToTwoDigits(vertex.data.doubleValue)
      if (vertex.id == 0) {
        assert(0.0 == doubleValue)
      } else if (vertex.id == 1) {
        assert(0.1 == doubleValue)
      } else if (vertex.id == 2) {
        assert(0.2 == doubleValue)
      } else if (vertex.id == 3) {
        assert(0.3 == doubleValue)
      } else if (vertex.id == 4) {
        assert(0.4 == doubleValue)
      } else if (vertex.id == 5) {
        assert(0.5 == doubleValue)
      } else if (vertex.id == 6) {
        assert(0.6 == doubleValue)
      } else if (vertex.id == 7) {
        assert(0.12 == doubleValue)
      }
    }
    sc.stop()
  }

  test("propagateForwardFixedNumberOfIterationsTwoIterationsTest") {
    var (sc, g) = loadGraphAndRunPropagateFixNumberOfIterations(2, EdgeDirection.Out,
      "propagateForwardFixedNumberOfIterationsTwoIterationTest")
    collectVerticesAndVerifyTwoIterationsOfDoubleValuePropagationForwardAndStopSparkContext(sc, g)
  }
//
//  test("propagateForwardFixedNumberOfIterationsReflectionTwoIterationsTest") {
//    var (sc, g) = loadGraphAndRunPropagateFixNumberOfIterationsReflection(2, EdgeDirection.Out,
//      "propagateForwardFixedNumberOfIterationsReflectionTwoIterationsTest")
//    collectVerticesAndVerifyTwoIterationsOfDoubleValuePropagationForwardAndStopSparkContext(sc, g)
//  }

  private def collectVerticesAndVerifyTwoIterationsOfDoubleValuePropagationForwardAndStopSparkContext(
    sc: SparkContext,
    g: Graph[DoubleIntInt, Double]) = {
    val localVertices = g.vertices.collect()
    for (vertex <- localVertices) {
      checkIntValue1AndIntValue2AreUnchanged(vertex)
      val doubleValue = getDoubleValueRoundedToTwoDigits(vertex.data.doubleValue)
      if (vertex.id == 0) {
        assert(0.0 == doubleValue)
      } else if (vertex.id == 1) {
        assert(0.05 == doubleValue)
      } else if (vertex.id == 2) {
        assert(0.11 == doubleValue)
      } else if (vertex.id == 3) {
        assert(0.3 == doubleValue)
      } else if (vertex.id == 4) {
        assert(0.4 == doubleValue)
      } else if (vertex.id == 5) {
        assert(0.5 == doubleValue)
      } else if (vertex.id == 6) {
        assert(0.6 == doubleValue)
      } else if (vertex.id == 7) {
        assert(0.7 == doubleValue)
      }
    }
    sc.stop()
  }

  test("propagateInTransposeFixedNumberOfIterationsTwoIterationTest") {
    var (sc, g) = loadGraphAndRunPropagateFixNumberOfIterations(2, EdgeDirection.In,
      "propagateFixedNumberOfIterationsReflection")
    collectVerticesAndVerifyTwoIterationsOfDoubleValuePropagationInTransposeAndStopSparkContext(sc, g)
  }
//  
//  test("propagateInTransposeFixedNumberOfIterationsReflectionTwoIterationTest") {
//    var (sc, g) = loadGraphAndRunPropagateFixNumberOfIterationsReflection(2, EdgeDirection.In,
//      "propagateInTransposeFixedNumberOfIterationsReflectionTwoIterationTest")
//    collectVerticesAndVerifyTwoIterationsOfDoubleValuePropagationInTransposeAndStopSparkContext(sc, g)
//  }

  private def collectVerticesAndVerifyTwoIterationsOfDoubleValuePropagationInTransposeAndStopSparkContext(
    sc: SparkContext,
    g: Graph[DoubleIntInt, Double]) = {
        val localVertices = g.vertices.collect()
    for (vertex <- localVertices) {
      checkIntValue1AndIntValue2AreUnchanged(vertex)
      val doubleValue = getDoubleValueRoundedToTwoDigits(vertex.data.doubleValue)
      if (vertex.id == 0) {
        assert(0.0 == doubleValue)
      } else if (vertex.id == 1) {
        assert(0.1 == doubleValue)
      } else if (vertex.id == 2) {
        assert(0.2 == doubleValue)
      } else if (vertex.id == 3) {
        assert(0.3 == doubleValue)
      } else if (vertex.id == 4) {
        assert(0.4 == doubleValue)
      } else if (vertex.id == 5) {
        assert(0.5 == doubleValue)
      } else if (vertex.id == 6) {
        assert(0.23 == doubleValue)
      } else if (vertex.id == 7) {
        assert(0.12 == doubleValue)
      }
    }
    sc.stop()
  }

  test("propagateForwardFixedNumberOfIterationsSevenIterationsTest") {
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(7, EdgeDirection.Out,
      "propagateForwardFixedNumberOfIterationsSevenIterationsTest")
  }

  test("propagateForwardFixedNumberOfIterationsReflectionSevenIterationsTest") {
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(7, EdgeDirection.Out,
      "propagateForwardFixedNumberOfIterationsReflectionSevenIterationsTest")
  }

  test("propagateFixedNumberOfIterationsMoreThanSevenIterationsTest") {
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(8, EdgeDirection.Out,
      "propagateFixedNumberOfIterationsEightIterationsTest")
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(9, EdgeDirection.Out,
      "propagateFixedNumberOfIterationsNineIterationsTest")
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(10, EdgeDirection.Out,
      "propagateFixedNumberOfIterationsTenIterationsTest")
  }

  test("propagateFixedNumberOfIterationsReflectionMoreThanSevenIterationsTest") {
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(8, EdgeDirection.Out,
      "propagateFixedNumberOfIterationsEightIterationsTest")
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(9, EdgeDirection.Out,
      "propagateFixedNumberOfIterationsNineIterationsTest")
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(10, EdgeDirection.Out,
      "propagateFixedNumberOfIterationsTenIterationsTest")
  }

  test("propagateInTransposeFixedNumberOfIterationsSevenIterationsTest") {
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(7, EdgeDirection.In,
      "propagateInTransposeFixedNumberOfIterationsSevenIterationsTest")
  }

  test("propagateInTransposeFixedNumberOfIterationsReflectionSevenIterationsTest") {
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(7, EdgeDirection.In,
      "propagateInTransposeFixedNumberOfIterationsSevenIterationsTest")
  }

  test("propagateInTransposeFixedNumberOfIterationsMoreThanSevenIterationsTest") {
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(8, EdgeDirection.In,
      "propagateInTransposeFixedNumberOfIterationsEightIterationsTest")
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(9, EdgeDirection.In,
      "propagateInTransposeFixedNumberOfIterationsNineIterationsTest")
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(10, EdgeDirection.In,
      "propagateInTransposeFixedNumberOfIterationsTenIterationsTest")
  }

  test("propagateInTransposeFixedNumberOfIterationsReflectionMoreThanSevenIterationsTest") {
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(8, EdgeDirection.In,
      "propagateInTransposeFixedNumberOfIterationsEightIterationsTest")
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(9, EdgeDirection.In,
      "propagateInTransposeFixedNumberOfIterationsNineIterationsTest")
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(10, EdgeDirection.In,
      "propagateInTransposeFixedNumberOfIterationsTenIterationsTest")
  }

  test("propagateInBothDirectionsFixedNumberOfIterationsFourIterationsTest") {
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(4, EdgeDirection.Both,
      "propagateInBothDirectionsFixedNumberOfIterationsFourIterationsTest")
  }

  test("propagateInBothDirectionsFixedNumberOfIterationsReflectionFourIterationsTest") {
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(4, EdgeDirection.Both,
      "propagateInBothDirectionsFixedNumberOfIterationsReflectionFourIterationsTest")
  }

  test("propagateInBothDirectionsFixedNumberOfIterationsMoreThanFourIterationsTest") {
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(5, EdgeDirection.Both,
      "propagateInBothDirectionsFixedNumberOfIterationsFiveIterationsTest")
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(7, EdgeDirection.Both,
      "propagateInBothDirectionsFixedNumberOfIterationsSevenIterationsTest")
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(10, EdgeDirection.Both,
      "propagateInBothDirectionsFixedNumberOfIterationsTenIterationsTest")
  }

  test("propagateInBothDirectionsFixedNumberOfIterationsReflectionMoreThanFourIterationsTest") {
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(5, EdgeDirection.Both,
      "propagateInBothDirectionsFixedNumberOfIterationsReflectionFiveIterationsTest")
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(7, EdgeDirection.Both,
      "propagateInBothDirectionsFixedNumberOfIterationsReflectionSevenIterationsTest")
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(10, EdgeDirection.Both,
      "propagateInBothDirectionsFixedNumberOfIterationsReflectionTenIterationsTest")
  }

  test("propagateForwardUntilConvergenceTest") {
    doPropagateUntilConvergenceTest(EdgeDirection.Out, "propagateForwardUntilConvergenceTest")
  }

  test("propagateForwardUntilConvergenceReflectionTest") {
    doPropagateUntilConvergenceTest(EdgeDirection.Out, "propagateForwardUntilConvergenceReflectionTest")
  }

  test("propagateInTransposeUntilConvergenceTest") {
    doPropagateUntilConvergenceTest(EdgeDirection.In, "propagateInTransposeUntilConvergenceTest")
  }

  test("propagateInTransposeUntilConvergenceReflectionTest") {
    doPropagateUntilConvergenceTest(EdgeDirection.In, "propagateInTransposeUntilConvergenceReflectionTest")
  }

  test("propagateInBothDirectionsUntilConvergenceTest") {
    doPropagateUntilConvergenceTest(EdgeDirection.Both, "propagateInBothDirectionsUntilConvergenceTest")
  }

  test("propagateInBothDirectionsUntilConvergenceReflectionTest") {
    doPropagateUntilConvergenceTest(EdgeDirection.Both, "propagateInBothDirectionsUntilConvergenceReflectionTest")
  }
//  
//  test("simplePropagateTest") {
//    doSimplePropagateTest(EdgeDirection.Out)
//    doSimplePropagateTest(EdgeDirection.In)
//    doSimplePropagateTest(EdgeDirection.Both)
//  }
//
//  test("simplePropagateForwardFromOneTest") {
//	doSimplePropagateForwardFromOneTest(false /* do not use reflection */,
//	  "simplePropagateForwardFromOneTest")
//  }
//  
//  test("simplePropagateForwardFromOneReflectionTest") {
//	  doSimplePropagateForwardFromOneTest(true /* use reflection */,
//	    "simplePropagateForwardFromOneReflectionTest")
//  }
//  
//  private def doSimplePropagateForwardFromOneTest(testName: String) {
//    var (sc, g) = loadGraphFromVertexAndEdgeFileAndCache(testSmallNotConnectedGraphVertexFile,
//	  testSmallNotConnectedGraphEdgeFile, testName)
//    // Simply propagates 4's value (which is 10) around with max aggregation
//    g = {
////      if (useReflection) {
////        g.simplePropagateForwardFromOneReflection[Int](
////          4 /* start vertex */ ,
////          "intValue1",
////          maxAggr)
////      } else {
//        g.simplePropagateForwardFromOne[Int](
//          4 /* start vertex */ ,
//          v => v.intValue1,
//          maxAggr,
//          (vertexValue: DoubleIntInt, finalValue: Int) => {
//            vertexValue.intValue1 = finalValue
//            vertexValue
//          })
//      }
//    }
//    val localVertices = g.vertices.collect()
//    for (vertex <- localVertices) {
//      println("vertexId: " + vertex.id)
//      println("intValue1: " + vertex.data.intValue1)
//      println("intValue2: " + vertex.data.intValue2)
//      val actualDoubleVal = getDoubleValueRoundedToTwoDigits(vertex.data.doubleValue)
//      val expectedDoubleVal = getDoubleValueRoundedToTwoDigits(vertex.id/10.0)
//      println("actualDoubleVal: " + actualDoubleVal)
//      println("expectedDoubleVal: " + expectedDoubleVal)
//      assert(expectedDoubleVal == actualDoubleVal)
//      assert(-1 == vertex.data.intValue2)
//      if (vertex.id == 4 || vertex.id == 5 || vertex.id == 6)
//    	  assert(10 == vertex.data.intValue1)
//      else if (vertex.id == 0) assert(3 == vertex.data.intValue1)
//      else if (vertex.id == 1) assert(2 == vertex.data.intValue1)
//      else if (vertex.id == 2) assert(1 == vertex.data.intValue1)
//      else if (vertex.id == 7) assert(11 == vertex.data.intValue1)
//    }
//    sc.stop()
//  }

  test("propagateUntilConvergenceNoEdgeValuesInTransposeFromOneTest") {
    var (sc, g) = loadGraphFromVertexAndEdgeFileAndCache(testSmallNotConnectedGraphVertexFile,
	  testSmallNotConnectedGraphEdgeFile, "propagateUntilConvergenceNoEdgeValuesInTransposeFromOneTest")
    // Simply propagates 5's value (which is 7) around with max aggregation
    g =  g.propagateAndAggregateUntilConvergence[Int](
      EdgeDirection.In,
      v => v.id == 4 /* start vertex */ ,
      v => v.intValue1,
      (msg, evals) => msg,
      (v, msgs) => { v.data.intValue1 = maxAggr(v.data.intValue1, msgs); v.data })

    val localVertices = g.vertices.collect()
    for (vertex <- localVertices) {
      println("vertexId: " + vertex.id)
      println("intValue1: " + vertex.data.intValue1)
      println("intValue2: " + vertex.data.intValue2)
      val actualDoubleVal = getDoubleValueRoundedToTwoDigits(vertex.data.doubleValue)
      val expectedDoubleVal = getDoubleValueRoundedToTwoDigits(vertex.id/10.0)
      println("actualDoubleVal: " + actualDoubleVal)
      println("expectedDoubleVal: " + expectedDoubleVal)
      assert(expectedDoubleVal == actualDoubleVal)
      assert(-1 == vertex.data.intValue2)
      if (vertex.id <= 4)
    	  assert(10 == vertex.data.intValue1)
      else if (vertex.id == 7) assert(11 == vertex.data.intValue1)
      else assert(((vertex.id + 2) % 8) == vertex.data.intValue1)
    }
    sc.stop()
  }

  test("propagateUntilConvergenceNoEdgeValuesInBothDirectionsFromOneTest") {
    var (sc, g) = loadGraphFromVertexAndEdgeFileAndCache(testSmallNotConnectedGraphVertexFile,
      testSmallNotConnectedGraphEdgeFile, "propagateUntilConvergenceNoEdgeValuesInBothDirectionsFromOneTest")
    // Simply propagates 5's value (which is 7) around with max aggregation
    g = g.propagateAndAggregateUntilConvergence[Int](
      EdgeDirection.Both,
      v => v.id == 4 /* start vertex */ ,
      v => v.intValue1,
      (msg, e) => msg,
      (v, msgs) => { v.data.intValue1 = maxAggr(v.data.intValue1, msgs); v.data })
    val localVertices = g.vertices.collect()
    for (vertex <- localVertices) {
      println("vertexId: " + vertex.id)
      println("intValue1: " + vertex.data.intValue1)
      println("intValue2: " + vertex.data.intValue2)
      val actualDoubleVal = getDoubleValueRoundedToTwoDigits(vertex.data.doubleValue)
      val expectedDoubleVal = getDoubleValueRoundedToTwoDigits(vertex.id / 10.0)
      println("actualDoubleVal: " + actualDoubleVal)
      println("expectedDoubleVal: " + expectedDoubleVal)
      assert(expectedDoubleVal == actualDoubleVal)
      assert(-1 == vertex.data.intValue2)
      if (vertex.id == 7) assert(11 == vertex.data.intValue1)
      else assert(10 == vertex.data.intValue1)
    }
    sc.stop()
  }
    
  test("formSuperVerticesSelfLoopRemovalMaxEdgeMaxVertexValuesTest") {
	  doFormSuperVerticesNoSelfLoopRemovalMaxEdgeValuesTest(true /* remove self loops */,
	    true /* use max for aggregating edges */, true /* use max for aggregating vertices */)
  }
    
  test("formSuperVerticesNoSelfLoopRemovalMaxEdgeMaxVertexValuesTest") {
	  doFormSuperVerticesNoSelfLoopRemovalMaxEdgeValuesTest(false /* do not remove self loops */,
	    true /* use max for aggregating edges */, true /* use max for aggregating vertices */)
  }
  
  test("formSuperVerticesSelfLoopRemovalMinEdgeMaxVertexValuesTest") {
	  doFormSuperVerticesNoSelfLoopRemovalMaxEdgeValuesTest(true /* remove self loops */,
	    false /* use min for aggregating edges */, true /* use max for aggregating vertices */)
  }

  test("formSuperVerticesSelfLoopRemovalMaxEdgeMinVertexValuesTest") {
	  doFormSuperVerticesNoSelfLoopRemovalMaxEdgeValuesTest(true /* remove self loops */,
	    true /* use max for aggregating edges */, false /* use min for aggregating vertices */)
  }

    
  test("updateVerticesUsingLocalEdgesMaxAggrTest") {
    doUpdateVerticesUsingLocalEdgesTest(true /* use max to aggregate edges */)
  }

  test("updateVerticesUsingLocalEdgesMinAggrTest") {
    doUpdateVerticesUsingLocalEdgesTest(false /* use min to aggregate edges */)
  }
  
  private def doUpdateVerticesUsingLocalEdgesTest(useMaxAggr: Boolean) {
    val sc = new SparkContext("local", "updateVerticesUsingLocalEdgesTest")
    var g = GraphLoader.textFileWithVertexValues(sc, smallIntegerWeightedGraphVerticesFileName,
      smallIntegerWeightedGraphEdgesFileName,
      (id, values) => -1, (srcDstId, evalsString) => evalsString(0).toInt)
    println("printing edges before calling updateVertices...")
    println(g.edges.collect().deep.mkString("\n"))
    println("finished printing edges before calling updateVertices...")
    g = g.updateVerticesUsingLocalEdges(
      EdgeDirection.Out,
      (vertex, edgesList) => {
      var finalValue = vertex.data
      if (!edgesList.isEmpty) {
        if (useMaxAggr) {
          for (edge <- edgesList.get) {
            if (edge.data > finalValue) finalValue = edge.data
          }
        } else {
          for (edge <- edgesList.get) {
            if (finalValue < 0 || edge.data < finalValue) finalValue = edge.data
          }
        }
      }
      finalValue
    })
    val localVertices = g.vertices.collect()
    assert(10 == localVertices.size)
    for (vertex <- localVertices) {
      println("vertexId: " + vertex.id)
      println("vertexValue: " + vertex.data)
      if (vertex.id == 0) { assert(3 == vertex.data) }
      else if (vertex.id == 1) { assert(1 == vertex.data) }
      else if (vertex.id == 2) { assert(2 == vertex.data) }
      else if (vertex.id == 3) { assert(-1 == vertex.data) }
      else if (vertex.id == 4) { assert(20 == vertex.data) }
      else if (vertex.id == 5) { assert((if (useMaxAggr) 7 else 2) == vertex.data) }
      else if (vertex.id == 6) { assert(3 == vertex.data) }
      else if (vertex.id == 7) { assert(4 == vertex.data) }
      else if (vertex.id == 8) { assert(-1 == vertex.data) }
      else if (vertex.id == 9) { assert(-1 == vertex.data) }
      else assert(false)
    }
    sc.stop()
  }

  private def doFormSuperVerticesNoSelfLoopRemovalMaxEdgeValuesTest(removeSelfLoops: Boolean,
    useMaxEdgesAggr: Boolean, useMaxVerticesAggr: Boolean) = {
    val sc = new SparkContext("local", "formSuperVerticesNoSelfLoopRemovalMaxEdgeValuesTest")
    var g = GraphLoader.textFileWithVertexValues(sc, formSuperverticesTestVerticesFileName,
      formSuperverticesTestEdgesFileName,
      (id, values) => new DoubleIntInt(-1.0, values(0).toInt, -1), (srcDstId, evalsString) => evalsString(0).toDouble)//.cache()
    g = g.updateVertices{ v => v.data.doubleValue = v.id.toDouble; v.data }
    g = g.formSuperVertices(
      v => v.intValue1 /* group by intValue1 */,
      if (useMaxEdgesAggr) maxAggrSeq else minAggrSeq,
      vertexValues => {
        var finalVertexValue = vertexValues(0)
        for (index <- 1 to (vertexValues.length - 1)) {
          val vertexValue = vertexValues(index)
          if (useMaxVerticesAggr) {
            if (vertexValue.doubleValue > finalVertexValue.doubleValue) {
              finalVertexValue = vertexValue
            }
          } else {
            if (vertexValue.doubleValue < finalVertexValue.doubleValue) {
              finalVertexValue = vertexValue
            }            
          }
        }
        finalVertexValue 
      },
      removeSelfLoops)
    val localVertices = g.vertices.collect()
    assert(5 == localVertices.size)
    var firstVFound, secondVFound, thirdVFound, fourthVFound, fifthVFound = false
    for (vertex <- localVertices) {
      println("vertexId: " + vertex.id)
      println("intValue1: " + vertex.data.intValue1)
      println("intValue2: " + vertex.data.intValue2)
      val actualDoubleVal = getDoubleValueRoundedToTwoDigits(vertex.data.doubleValue)
      println("actualDoubleVal: " + actualDoubleVal)
      assert(vertex.id == vertex.data.intValue1)
      assert(-1 == vertex.data.intValue2)
      if (vertex.id == 0) { assert(0.0 == actualDoubleVal); firstVFound = true; }
      else if (vertex.id == 1) { assert((if (useMaxVerticesAggr) 3.0 else 1.0) == actualDoubleVal); secondVFound = true; }
      else if (vertex.id == 6) { assert((if (useMaxVerticesAggr) 7.0 else 4.0) == actualDoubleVal); thirdVFound = true; }
      else if (vertex.id == 9) { assert((if (useMaxVerticesAggr) 9.0 else 8.0) == actualDoubleVal); fourthVFound = true; }
      else if (vertex.id == 10) { assert(10.0 == actualDoubleVal); fifthVFound = true; }
      else assert(false)
    }
    assert(firstVFound)
    assert(secondVFound)
    assert(thirdVFound)
    assert(fourthVFound)
    assert(fifthVFound)

    val localEdges = g.edges.collect()
    assert(5 == localVertices.size)
    var firstEFound, secondEFound, thirdEFound, fourthEFound, fifthEFound, sixthEFound, seventhEFound, eighthEFound = false
    for (edge <- localEdges) {
      println("srcId: " + edge.src)
      println("destId: " + edge.dst)
      val actualWeight = getDoubleValueRoundedToTwoDigits(edge.data)
      println("weight: " + actualWeight)
      if (edge.src == 0 && edge.dst == 1) { assert(0.1 == actualWeight); firstEFound = true }
      else if (edge.src == 1 && edge.dst == 6) { assert(2.4 == actualWeight); secondEFound = true }
      else if (edge.src == 6 && edge.dst == 10) { assert(4.1 == actualWeight); thirdEFound = true }
      else if (edge.src == 6 && edge.dst == 9) { assert(5.8 == actualWeight); fourthEFound = true }
      else if (edge.src == 9 && edge.dst == 6) {
        assert((if (useMaxEdgesAggr) 9.4 else 8.6) == actualWeight); fifthEFound = true }
      else if (!removeSelfLoops && edge.src == 1 && edge.dst == 1) {
        assert((if (useMaxEdgesAggr) 3.2 else 1.3) == actualWeight); sixthEFound = true }
      else if (!removeSelfLoops && edge.src == 6 && edge.dst == 6) {
        assert((if (useMaxEdgesAggr) 7.5 else 4.5) == actualWeight); seventhEFound = true }
      else if (!removeSelfLoops && edge.src == 9 && edge.dst == 9) {
        assert((if (useMaxEdgesAggr) 9.8 else 8.9) == actualWeight); eighthEFound = true }
      else assert(false)
    }
    assert(firstEFound)
    assert(secondEFound)
    assert(thirdEFound)
    assert(fourthEFound)
    assert(fifthEFound)
    if (!removeSelfLoops) {
      assert(sixthEFound)
      assert(seventhEFound)
      assert(eighthEFound)
    }
    sc.stop()
  }

  test("propagateUntilConvergenceNoEdgeValuesForwardFromAllTest") {
    var (sc, g) = loadGraphFromVertexAndEdgeFileAndCache(testSmallNotConnectedGraphVertexFile,
	  testSmallNotConnectedGraphEdgeFile, "simplePropagateForwardFromAllTest")
    g = g.propagateAndAggregateUntilConvergence[Int](
      EdgeDirection.Out,
      v => true,
      v => v.intValue1,
      (msg, evals) => msg,
      (v, msgs) => {
        v.data.intValue1 = maxAggr(v.data.intValue1, msgs)
        v.data
      })
    val localVertices = g.vertices.collect()
    for (vertex <- localVertices) {
      println("vertexId: " + vertex.id)
      println("intValue1: " + vertex.data.intValue1)
      println("intValue2: " + vertex.data.intValue2)
      val actualDoubleVal = getDoubleValueRoundedToTwoDigits(vertex.data.doubleValue)
      val expectedDoubleVal = getDoubleValueRoundedToTwoDigits(vertex.id/10.0)
      println("actualDoubleVal: " + actualDoubleVal)
      println("expectedDoubleVal: " + expectedDoubleVal)
      assert(expectedDoubleVal == actualDoubleVal)
      assert(-1 == vertex.data.intValue2)
      if (vertex.id == 0 || vertex.id == 1 || vertex.id == 2) assert(3 == vertex.data.intValue1)
      else if (vertex.id == 3) assert(5 == vertex.data.intValue1)
      else if (vertex.id == 7) assert(11 == vertex.data.intValue1)
      else assert(10 == vertex.data.intValue1)
    }
    sc.stop()
  }

  test("propagateUntilConvergenceNoEdgeValuesInTransposeFromAllTest") {
    var (sc, g) = loadGraphFromVertexAndEdgeFileAndCache(testSmallNotConnectedGraphVertexFile,
	  testSmallNotConnectedGraphEdgeFile, "propagateUntilConvergenceNoEdgeValuesForwardFromAllTest")
    g = g.propagateAndAggregateUntilConvergence[Int](
      EdgeDirection.In,
      v => true,
      v => v.intValue1,
      (msg, evals) => msg,
      (v, msgs) => {
         v.data.intValue1 = maxAggr(v.data.intValue1, msgs)
         v.data
     })
    val localVertices = g.vertices.collect()
    for (vertex <- localVertices) {
      println("vertexId: " + vertex.id)
      println("intValue1: " + vertex.data.intValue1)
      println("intValue2: " + vertex.data.intValue2)
      val actualDoubleVal = getDoubleValueRoundedToTwoDigits(vertex.data.doubleValue)
      val expectedDoubleVal = getDoubleValueRoundedToTwoDigits(vertex.id/10.0)
      println("actualDoubleVal: " + actualDoubleVal)
      println("expectedDoubleVal: " + expectedDoubleVal)
      assert(expectedDoubleVal == actualDoubleVal)
      assert(-1 == vertex.data.intValue2)
      if (vertex.id == 5) assert(7 == vertex.data.intValue1)
      else assert(11 == vertex.data.intValue1)
    }
    sc.stop()
  }

  test("propagateUntilConvergenceNoEdgeValuesInBothDirectionsFromAllTest") {
    var (sc, g) = loadGraphFromVertexAndEdgeFileAndCache(testSmallNotConnectedGraphVertexFile,
      testSmallNotConnectedGraphEdgeFile, "propagateUntilConvergenceNoEdgeValuesInBothDirectionsFromAllTest")
    g = g.propagateAndAggregateUntilConvergence[Int](
      EdgeDirection.Both,
      v => true,
      v => v.intValue1,
      (msg, evals) => msg,
      (v, msgs) => {
        v.data.intValue1 = maxAggr(v.data.intValue1, msgs);
        v.data
      })

    val localVertices = g.vertices.collect()
    for (vertex <- localVertices) {
      println("vertexId: " + vertex.id)
      println("intValue1: " + vertex.data.intValue1)
      println("intValue2: " + vertex.data.intValue2)
      val actualDoubleVal = getDoubleValueRoundedToTwoDigits(vertex.data.doubleValue)
      val expectedDoubleVal = getDoubleValueRoundedToTwoDigits(vertex.id / 10.0)
      println("actualDoubleVal: " + actualDoubleVal)
      println("expectedDoubleVal: " + expectedDoubleVal)
      assert(expectedDoubleVal == actualDoubleVal)
      assert(-1 == vertex.data.intValue2)
      assert(11 == vertex.data.intValue1)
    }
    sc.stop()
  }
//
//  private def doSimplePropagateTest(direction: EdgeDirection) = {
//    var (sc, g) = loadGraphFromVertexAndEdgeFileAndCache(testSmallGraphVertexFile, testSmallGraphEdgeFile,
//      "simplePropagateTest")
//    // Simply propagates 4 around with null aggregation
//    g = g.simplePropagate(direction,
//      v => v.id == 4,
//      v => v.intValue1,
//      (vertexField: Int, msgs: Seq[Int]) => { msgs(0) },
//      (vertexValue, finalValue: Int) => {
//        vertexValue.intValue1 = finalValue
//        vertexValue
//      })
//    verifyThatValuesHaveConvergedForSimplePropagationTestAndStopContext(sc, g, direction)
//  }

  private def checkIntValue1AndIntValue2AreUnchanged(vertex: Vertex[DoubleIntInt]) = {
    assert(-1 == vertex.data.intValue2)
    assert(((vertex.id + 2) % 8) == vertex.data.intValue1)
  }

  private def doPropagateUntilConvergenceTest(direction: EdgeDirection, testName: String) {
    var (sc, g) = loadGraphFromVertexAndEdgeFileAndCache(testSmallGraphVertexFile, testSmallGraphEdgeFile,
      testName)
    g = g.propagateAndAggregateUntilConvergence[Double](
          direction,
          v => v.id == 0,
          value => value.doubleValue,
          (propagatedValue, edgeValue) => propagatedValue + edgeValue,
          (v, msgs) => {
            v.data.doubleValue = minAggr(v.data.doubleValue, msgs)
            v.data
          })
    verifyThatValuesHaveConvergedForPropagationTestAndStopContext(sc, g, direction)
  }

  private def doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(numIter: Int,
    direction: EdgeDirection, testName: String) {
    var (sc, g) = loadGraphAndRunPropagateFixNumberOfIterations(numIter, direction, testName)
    verifyThatValuesHaveConvergedForPropagationTestAndStopContext(sc, g, direction)
  }

  private def verifyThatValuesHaveConvergedForSimplePropagationTestAndStopContext(
    sc: SparkContext,
    g: Graph[DoubleIntInt, Double],
    direction: EdgeDirection) {
    val localVertices = g.vertices.collect()
    for (vertex <- localVertices) {
      println("intValue1: " + vertex.data.intValue1)
      println("intValue2: " + vertex.data.intValue2)
      val actualDoubleVal = getDoubleValueRoundedToTwoDigits(vertex.data.doubleValue)
      val expectedDoubleVal = getDoubleValueRoundedToTwoDigits(vertex.id/10.0)
      println("actualDoubleVal: " + actualDoubleVal)
      println("expectedDoubleVal: " + expectedDoubleVal)
      assert(expectedDoubleVal == actualDoubleVal)
      assert(-1 == vertex.data.intValue2)
      assert(6 == vertex.data.intValue1)
    }
    sc.stop()
  }
  
  private def verifyThatValuesHaveConvergedForPropagationTestAndStopContext(
    sc: SparkContext,
    g: Graph[DoubleIntInt, Double],
    direction: EdgeDirection) {
    val localVertices = g.vertices.collect()
    for (vertex <- localVertices) {
      println("intValue1: " + vertex.data.intValue1)
      println("intValue2: " + vertex.data.intValue2)
      checkIntValue1AndIntValue2AreUnchanged(vertex)
      val doubleVal = getDoubleValueRoundedToTwoDigits(vertex.data.doubleValue)
      println("doubleVal: " + doubleVal)
      if (vertex.id == 0) {
        assert(0.0 == doubleVal)
      } else if (vertex.id == 1) {
        direction match {
          case EdgeDirection.Out => assert(0.05 == doubleVal)
          case EdgeDirection.In => assert(0.1 == doubleVal)
          case EdgeDirection.Both => assert(0.05 == doubleVal)
        }
      } else if (vertex.id == 2) {
        direction match {
          case EdgeDirection.Out => assert(0.11 == doubleVal)
          case EdgeDirection.In => assert(0.2 == doubleVal)
          case EdgeDirection.Both => assert(0.11 == doubleVal)
        }
      } else if (vertex.id == 3) {
        direction match {
          case EdgeDirection.Out => assert(0.18 == doubleVal)
          case EdgeDirection.In => assert(0.3 == doubleVal)
          case EdgeDirection.Both => assert(0.18 == doubleVal)
        }
      } else if (vertex.id == 4) {
        direction match {
          case EdgeDirection.Out => assert(0.26 == doubleVal)
          case EdgeDirection.In => assert(0.4 == doubleVal)
          case EdgeDirection.Both => assert(0.26 == doubleVal)
        }
      } else if (vertex.id == 5) {
        direction match {
          case EdgeDirection.Out => assert(0.35 == doubleVal)
          case EdgeDirection.In => assert(0.33 == doubleVal)
          case EdgeDirection.Both => assert(0.33 == doubleVal)
        }
      } else if (vertex.id == 6) {
        direction match {
          case EdgeDirection.Out => assert(0.45 == doubleVal)
          case EdgeDirection.In => assert(0.23 == doubleVal)
          case EdgeDirection.Both => assert(0.23 == doubleVal)
        }
      } else if (vertex.id == 7) {
        direction match {
          case EdgeDirection.Out => assert(0.56 == doubleVal)
          case EdgeDirection.In => assert(0.12 == doubleVal)
          case EdgeDirection.Both => assert(0.12 == doubleVal)
        }
      }
    }
    sc.stop()
  }

  private def getDoubleValueRoundedToTwoDigits(doubleValue: Double): Double = {
    scala.math.BigDecimal(doubleValue).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  private def loadGraphAndRunPropagateFixNumberOfIterations(numIter: Int, direction: EdgeDirection,
    testName: String) = {
    var (sc, g) = loadGraphFromVertexAndEdgeFileAndCache(testSmallGraphVertexFile, testSmallGraphEdgeFile,
      testName)
    g = g.propagateAndAggregateFixedNumberOfIterations[Double](
      direction,
      v => v.id == 0,
      value => value.doubleValue,
      (propagatedValue, edgeValue) => propagatedValue + edgeValue,
      (v, msgs) => {
        v.data.doubleValue = minAggr(v.data.doubleValue, msgs)
        v.data
      },
      numIter)
    (sc, g)
  }
//  
//  private def loadGraphAndRunPropagateFixNumberOfIterationsReflection(numIter: Int, direction: EdgeDirection,
//    testName: String) = {
//    var (sc, g) = loadGraphFromVertexAndEdgeFileAndCache(testSmallGraphVertexFile, testSmallGraphEdgeFile,
//      testName)
//    g = g.propagateFixedNumberOfIterationsReflection[Double](
//      direction,
//      v => v.id == 0,
//      "doubleValue",
//      (propagatedValue, edgeValue) => propagatedValue + edgeValue,
//      (vertexValue, msgs) => {
//        var minValue = vertexValue
//        println("starting minValue: " + minValue + " msgs: " + msgs)
//        for (msg <- msgs) {
//          println("msg: " + msg)
//          print("min of minValue: " + minValue + " msg: " + msg + " is: ")
//          minValue = scala.math.min(minValue, msg)
//          println("" + minValue)
//        }
//        println("returning: " + minValue)
//        minValue
//      },
//      numIter)
//    (sc, g)
//  }

  private def loadGraphFromVertexAndEdgeFileAndCache(
    vertexFileName: String, edgeFileName: String, testName: String): (spark.SparkContext, Graph[DoubleIntInt, Double]) = {
    val sc = new SparkContext(host, testName)
    var g = GraphLoader.textFileWithVertexValues(sc, vertexFileName, edgeFileName,
      (id, values) => new DoubleIntInt(values(0).trim().toDouble, values(1).trim().toInt, -1),
      (srcDstId, evalsString) => evalsString(0).toDouble)//.withPartitioner(numVPart, numEPart).cache()
    (sc, g)
  }

  private def loadGraphAndCache(fname: String, testName: String): (spark.SparkContext, spark.graph.Graph[Int, Double]) = {
    val sc = new SparkContext(host, testName)
    var g = GraphLoader.textFile(sc, fname, a => a(0).toDouble)//.withPartitioner(numVPart, numEPart).cache()
    (sc, g)
  }

  private def assertNumVerticesAndEdgesAndStop(sc: spark.SparkContext, g: spark.graph.Graph[Int, Double],
    expectedNumVertices: Int, expectedNumEdges: Int): Unit = {
    val numVertices = g.numVertices
    val numEdges = g.numEdges
    println("After: numVertices: " + numVertices + " numEdges: " + numEdges)
    assert(expectedNumVertices == numVertices)
    assert(expectedNumEdges == numEdges)
    sc.stop()
  }
}
