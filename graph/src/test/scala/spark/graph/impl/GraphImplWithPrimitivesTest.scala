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
    
  test("filterEdgesTest") {
    System.setProperty("spark.serializer", "spark.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "spark.bagel.examples.PRKryoRegistrator")
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
    g = g.filterVerticesBasedOnEdgeValues(EdgeDirection.Out,
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
    g = g.filterVerticesBasedOnEdgeValues(EdgeDirection.In,
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
    g = g.filterVerticesBasedOnEdgeValues(EdgeDirection.Both,
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
    g = g.updateVertexValueBasedOnAnotherVertexsValue(v => (v.id - 1 + 5) % 5,
      (neighborValue, ownVertex) => neighborValue)

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

  test("updateVertexValueBasedOnAnotherVertexsValueReflectionOverwriteDoubleTest") {
    var (sc, g) = loadGraphFromVertexAndEdgeFileAndCache(testSmallGraphVertexFile, testSmallGraphEdgeFile,
      "updateVertexValueBasedOnAnotherVertexsValueReflectionOverwriteDoubleTest")
    // 0 points to 2, 2->4, 4->6, 6->0, and 1->3, 3->5, 5->7, 7->1
    g = g.updateVertexValueBasedOnAnotherVertexsValueReflection("intValue1", "doubleValue", "doubleValue")
    assert(8 == g.numEdges)
    assert(8 == g.numVertices)
    val localVertices = g.vertices.collect()
    for (vertex <- localVertices) {
      if (vertex.id == 0) {
        assert(0.2 == vertex.data.doubleValue) // previously vertex 0 used to have doubleValue 0.0
      } else if (vertex.id == 1) {
        assert(0.3 == vertex.data.doubleValue) // previously vertex 1 used to have doubleValue 0.1
      } else if (vertex.id == 2) {
        assert(0.4 == vertex.data.doubleValue) // previously vertex 2 used to have doubleValue 0.2
      } else if (vertex.id == 3) {
        assert(0.5 == vertex.data.doubleValue) // previously vertex 3 used to have doubleValue 0.3
      } else if (vertex.id == 4) {
        assert(0.6 == vertex.data.doubleValue) // previously vertex 4 used to have doubleValue 0.4
      } else if (vertex.id == 5) {
        assert(0.7 == vertex.data.doubleValue) // previously vertex 5 used to have doubleValue 0.5
      } else if (vertex.id == 6) {
        assert(0.0 == vertex.data.doubleValue) // previously vertex 6 used to have doubleValue 0.6
      } else if (vertex.id == 7) {
        assert(0.1 == vertex.data.doubleValue) // previously vertex 7 used to have doubleValue 0.7
      }
    }
    sc.stop()
  }

  test("updateVertexValueBasedOnAnotherVertexsValueReflectionOverwriteIntTest") {
    var (sc, g) = loadGraphFromVertexAndEdgeFileAndCache(testSmallGraphVertexFile, testSmallGraphEdgeFile,
      "updateVertexValueBasedOnAnotherVertexsValueReflectionTest")
    g = g.updateVertexValueBasedOnAnotherVertexsValueReflection("intValue1", "intValue1", "intValue1")
    assert(8 == g.numEdges)
    assert(8 == g.numVertices)
    val localVertices = g.vertices.collect()
    for (vertex <- localVertices) {
      assert(-1 == vertex.data.intValue2)
      if (vertex.id == 0) {
        assert(4 == vertex.data.intValue1) // previously vertex 0 used to have intValue1 2
      } else if (vertex.id == 1) {
        assert(5 == vertex.data.intValue1) // previously vertex 1 used to have intValue1 3
      } else if (vertex.id == 2) {
        assert(6 == vertex.data.intValue1) // previously vertex 2 used to have intValue1 4
      } else if (vertex.id == 3) {
        assert(7 == vertex.data.intValue1) // previously vertex 3 used to have intValue1 5
      } else if (vertex.id == 4) {
        assert(0 == vertex.data.intValue1) // previously vertex 4 used to have intValue1 6
      } else if (vertex.id == 5) {
        assert(1 == vertex.data.intValue1) // previously vertex 5 used to have intValue1 7
      } else if (vertex.id == 6) {
        assert(2 == vertex.data.intValue1) // previously vertex 6 used to have intValue1 0
      } else if (vertex.id == 7) {
        assert(3 == vertex.data.intValue1) // previously vertex 7 used to have intValue1 1
      }
    }
    sc.stop()
  }

  test("updateVertexValueBasedOnAnotherVertexsValueReflectionIntWritingToAnotherInt") {
    var (sc, g) = loadGraphFromVertexAndEdgeFileAndCache(testSmallGraphVertexFile, testSmallGraphEdgeFile,
      "updateVertexValueBasedOnAnotherVertexsValueReflectionIntWritingToAnotherInt")
    g = g.updateVertexValueBasedOnAnotherVertexsValueReflection("intValue1", "intValue1", "intValue2")
    assert(8 == g.numEdges)
    assert(8 == g.numVertices)
    val localVertices = g.vertices.collect()
    for (vertex <- localVertices) {
      if (vertex.id == 0) {
        assert(4 == vertex.data.intValue2) // previously vertex 0 used to have intValue2 -1
        assert(2 == vertex.data.intValue1)
      } else if (vertex.id == 1) {
        assert(5 == vertex.data.intValue2) // previously vertex 1 used to have intValue2 -1
        assert(3 == vertex.data.intValue1)
      } else if (vertex.id == 2) {
        assert(6 == vertex.data.intValue2) // previously vertex 2 used to have intValue2 -1
        assert(4 == vertex.data.intValue1)
      } else if (vertex.id == 3) {
        assert(7 == vertex.data.intValue2) // previously vertex 3 used to have intValue2 -1
        assert(5 == vertex.data.intValue1)
      } else if (vertex.id == 4) {
        assert(0 == vertex.data.intValue2) // previously vertex 4 used to have intValue2 -1
        assert(6 == vertex.data.intValue1)
      } else if (vertex.id == 5) {
        assert(1 == vertex.data.intValue2) // previously vertex 5 used to have intValue2 -1
        assert(7 == vertex.data.intValue1)
      } else if (vertex.id == 6) {
        assert(2 == vertex.data.intValue2) // previously vertex 6 used to have intValue2 -1
        assert(0 == vertex.data.intValue1)
      } else if (vertex.id == 7) {
        assert(3 == vertex.data.intValue2) // previously vertex 7 used to have intValue2 -1
        assert(1 == vertex.data.intValue1)
      }
    }
    sc.stop()
  }

  test("pickRandomVertexTest") {
    var (sc, g) = loadGraphAndCache("/Users/semihsalihoglu/projects/graphx/spark/test-data/test_pick_random_vertex.txt",
      "pickRandomVertexTest")
    val vertexId = g.pickRandomVertex()
    println("picked vertex id: " + vertexId)
    sc.stop()
  }

  test("propagateForwardFixedNumberOfIterationsOneIterationTest") {
    var (sc, g) = loadGraphAndRunPropagateFixNumberOfIterations(1, EdgeDirection.Out,
      "propagateForwardFixedNumberOfIterationsOneIterationTest")
	collectVerticesAndVerifyOneIterationOfDoubleValuePropagationForwardAndStopSparkContext(sc, g)  
  }

  test("propagateForwardFixedNumberOfIterationsReflectionOneIterationTest") {
    var (sc, g) = loadGraphAndRunPropagateFixNumberOfIterationsReflection(1, EdgeDirection.Out,
      "propagateForwardFixedNumberOfIterationsReflectionOneIterationTest")
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
  
  test("propagateInTransposeFixedNumberOfIterationsReflectionOneIterationTest") {
    var (sc, g) = loadGraphAndRunPropagateFixNumberOfIterationsReflection(1, EdgeDirection.In,
      "propagateInTransposeFixedNumberOfIterationsReflectionOneIterationTest")
    collectVerticesAndVerifyOneIterationOfDoubleValuePropagationInTransposeAndStopSparkContext(sc, g)
  }

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

  test("propagateForwardFixedNumberOfIterationsReflectionTwoIterationsTest") {
    var (sc, g) = loadGraphAndRunPropagateFixNumberOfIterationsReflection(2, EdgeDirection.Out,
      "propagateForwardFixedNumberOfIterationsReflectionTwoIterationsTest")
    collectVerticesAndVerifyTwoIterationsOfDoubleValuePropagationForwardAndStopSparkContext(sc, g)
  }

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
  
  test("propagateInTransposeFixedNumberOfIterationsReflectionTwoIterationTest") {
    var (sc, g) = loadGraphAndRunPropagateFixNumberOfIterationsReflection(2, EdgeDirection.In,
      "propagateInTransposeFixedNumberOfIterationsReflectionTwoIterationTest")
    collectVerticesAndVerifyTwoIterationsOfDoubleValuePropagationInTransposeAndStopSparkContext(sc, g)
  }

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
      "propagateForwardFixedNumberOfIterationsSevenIterationsTest", false /* do not use reflection */)
  }

  test("propagateForwardFixedNumberOfIterationsReflectionSevenIterationsTest") {
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(7, EdgeDirection.Out,
      "propagateForwardFixedNumberOfIterationsReflectionSevenIterationsTest", true /* use reflection */)
  }

  test("propagateFixedNumberOfIterationsMoreThanSevenIterationsTest") {
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(8, EdgeDirection.Out,
      "propagateFixedNumberOfIterationsEightIterationsTest", false /* do not use reflection */)
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(9, EdgeDirection.Out,
      "propagateFixedNumberOfIterationsNineIterationsTest", false /* do not use reflection */)
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(10, EdgeDirection.Out,
      "propagateFixedNumberOfIterationsTenIterationsTest", false /* do not use reflection */)
  }

  test("propagateFixedNumberOfIterationsReflectionMoreThanSevenIterationsTest") {
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(8, EdgeDirection.Out,
      "propagateFixedNumberOfIterationsEightIterationsTest", true /* use reflection */)
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(9, EdgeDirection.Out,
      "propagateFixedNumberOfIterationsNineIterationsTest", true /* use reflection */)
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(10, EdgeDirection.Out,
      "propagateFixedNumberOfIterationsTenIterationsTest", true /* use reflection */)
  }

  test("propagateInTransposeFixedNumberOfIterationsSevenIterationsTest") {
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(7, EdgeDirection.In,
      "propagateInTransposeFixedNumberOfIterationsSevenIterationsTest", false /* do not use reflection */)
  }

  test("propagateInTransposeFixedNumberOfIterationsReflectionSevenIterationsTest") {
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(7, EdgeDirection.In,
      "propagateInTransposeFixedNumberOfIterationsSevenIterationsTest", true /* use reflection */)
  }

  test("propagateInTransposeFixedNumberOfIterationsMoreThanSevenIterationsTest") {
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(8, EdgeDirection.In,
      "propagateInTransposeFixedNumberOfIterationsEightIterationsTest", false /* do not use reflection */)
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(9, EdgeDirection.In,
      "propagateInTransposeFixedNumberOfIterationsNineIterationsTest", false /* do not use reflection */)
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(10, EdgeDirection.In,
      "propagateInTransposeFixedNumberOfIterationsTenIterationsTest", false /* do not use reflection */)
  }

  test("propagateInTransposeFixedNumberOfIterationsReflectionMoreThanSevenIterationsTest") {
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(8, EdgeDirection.In,
      "propagateInTransposeFixedNumberOfIterationsEightIterationsTest", true /* use reflection */)
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(9, EdgeDirection.In,
      "propagateInTransposeFixedNumberOfIterationsNineIterationsTest", true /* use reflection */)
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(10, EdgeDirection.In,
      "propagateInTransposeFixedNumberOfIterationsTenIterationsTest", true /* use reflection */)
  }

  test("propagateInBothDirectionsFixedNumberOfIterationsFourIterationsTest") {
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(4, EdgeDirection.Both,
      "propagateInBothDirectionsFixedNumberOfIterationsFourIterationsTest", false /* do not use reflection */)
  }

  test("propagateInBothDirectionsFixedNumberOfIterationsReflectionFourIterationsTest") {
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(4, EdgeDirection.Both,
      "propagateInBothDirectionsFixedNumberOfIterationsReflectionFourIterationsTest", true /* use reflection */)
  }

  test("propagateInBothDirectionsFixedNumberOfIterationsMoreThanFourIterationsTest") {
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(5, EdgeDirection.Both,
      "propagateInBothDirectionsFixedNumberOfIterationsFiveIterationsTest", false /* do not use reflection */)
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(7, EdgeDirection.Both,
      "propagateInBothDirectionsFixedNumberOfIterationsSevenIterationsTest", false /* do not use reflection */)
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(10, EdgeDirection.Both,
      "propagateInBothDirectionsFixedNumberOfIterationsTenIterationsTest",  false /* do not use reflection */)
  }

  test("propagateInBothDirectionsFixedNumberOfIterationsReflectionMoreThanFourIterationsTest") {
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(5, EdgeDirection.Both,
      "propagateInBothDirectionsFixedNumberOfIterationsReflectionFiveIterationsTest", true /* use reflection */)
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(7, EdgeDirection.Both,
      "propagateInBothDirectionsFixedNumberOfIterationsReflectionSevenIterationsTest", true /* use reflection */)
    doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(10, EdgeDirection.Both,
      "propagateInBothDirectionsFixedNumberOfIterationsReflectionTenIterationsTest",  true /* use reflection */)
  }

  test("propagateForwardUntilConvergenceTest") {
    doPropagateUntilConvergenceTest(EdgeDirection.Out, false /* do not use reflection */,
      "propagateForwardUntilConvergenceTest")
  }

  test("propagateForwardUntilConvergenceReflectionTest") {
    doPropagateUntilConvergenceTest(EdgeDirection.Out, true /* use reflection */,
      "propagateForwardUntilConvergenceReflectionTest")
  }

  test("propagateInTransposeUntilConvergenceTest") {
    doPropagateUntilConvergenceTest(EdgeDirection.In, false /* do not use reflection */,
      "propagateInTransposeUntilConvergenceTest")
  }

  test("propagateInTransposeUntilConvergenceReflectionTest") {
    doPropagateUntilConvergenceTest(EdgeDirection.In, true /* use reflection */,
      "propagateInTransposeUntilConvergenceReflectionTest")
  }

  test("propagateInBothDirectionsUntilConvergenceTest") {
    doPropagateUntilConvergenceTest(EdgeDirection.Both, false /* do not use reflection */,
      "propagateInBothDirectionsUntilConvergenceTest")
  }

  test("propagateInBothDirectionsUntilConvergenceReflectionTest") {
    doPropagateUntilConvergenceTest(EdgeDirection.Both, true /* use reflection */,
      "propagateInBothDirectionsUntilConvergenceReflectionTest")
  }
  
  test("simplePropagateTest") {
    doSimplePropagateTest(EdgeDirection.Out)
    doSimplePropagateTest(EdgeDirection.In)
    doSimplePropagateTest(EdgeDirection.Both)
  }

  test("simplePropagateForwardFromOneTest") {
	  doSimplePropagateForwardFromOneTest(false /* do not use reflection */,
	    "simplePropagateForwardFromOneTest")
  }
  
  test("simplePropagateForwardFromOneReflectionTest") {
	  doSimplePropagateForwardFromOneTest(true /* use reflection */,
	    "simplePropagateForwardFromOneReflectionTest")
  }
  
  private def doSimplePropagateForwardFromOneTest(useReflection: Boolean,
    testName: String) {
    var (sc, g) = loadGraphFromVertexAndEdgeFileAndCache(testSmallNotConnectedGraphVertexFile,
	  testSmallNotConnectedGraphEdgeFile, testName)
    // Simply propagates 4's value (which is 10) around with max aggregation
    g = {
      if (useReflection) {
        g.simplePropagateForwardFromOneReflection[Int](
          4 /* start vertex */ ,
          "intValue1",
          maxAggr)
      } else {
        g.simplePropagateForwardFromOne[Int](
          4 /* start vertex */ ,
          v => v.intValue1,
          maxAggr,
          (vertexValue: DoubleIntInt, finalValue: Int) => {
            vertexValue.intValue1 = finalValue
            vertexValue
          })
      }
    }
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
      if (vertex.id == 4 || vertex.id == 5 || vertex.id == 6)
    	  assert(10 == vertex.data.intValue1)
      else if (vertex.id == 0) assert(3 == vertex.data.intValue1)
      else if (vertex.id == 1) assert(2 == vertex.data.intValue1)
      else if (vertex.id == 2) assert(1 == vertex.data.intValue1)
      else if (vertex.id == 7) assert(11 == vertex.data.intValue1)
    }
    sc.stop()
  }

  test("simplePropagateInTransposeFromOneTest") {
    doSimplePropagateInTransposeFromOneTest(false /* do not use reflection */,
      "simplePropagateInTransposeFromOneTest")
  }

  test("simplePropagateInTransposeFromOneReflectionTest") {
    doSimplePropagateInTransposeFromOneTest(true /* use reflection */,
      "simplePropagateInTransposeFromOneReflectionTest")
  }

  private def doSimplePropagateInTransposeFromOneTest(useReflection: Boolean, testName: String) {
    var (sc, g) = loadGraphFromVertexAndEdgeFileAndCache(testSmallNotConnectedGraphVertexFile,
	  testSmallNotConnectedGraphEdgeFile, "simplePropagateInTransposeFromOneTest")
    // Simply propagates 5's value (which is 7) around with max aggregation
    g = {
      if (useReflection) {
        g.simplePropagateInTransposeFromOneReflection[Int](
          4 /* start vertex */ ,
          "intValue1",
          maxAggr)
      } else {
        g.simplePropagateInTransposeFromOne[Int](
          4 /* start vertex */ ,
          v => v.intValue1,
          maxAggr,
          (vertexValue: DoubleIntInt, finalValue: Int) => {
            vertexValue.intValue1 = finalValue
            vertexValue
          })
      }
    }
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
    
  test("simplePropagateInBothDirectionsFromOneTest") {
	  doSimplePropagateInBothDirectionsFromOneTest(false /* do not use reflection */,
	    "simplePropagateInBothDirectionsFromOneTest")
  }
    
  test("simplePropagateInBothDirectionsFromOneReflectionTest") {
	  doSimplePropagateInBothDirectionsFromOneTest(true /* use reflection */,
	    "simplePropagateInBothDirectionsFromOneTest")
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

    
  test("updateVerticesBasedOnEdgeValuesMaxAggrTest") {
    doUpdateVerticesBasedOnEdgeValuesTest(true /* use max to aggregate edges */)
  }

  test("updateVerticesBasedOnEdgeValuesMinAggrTest") {
    doUpdateVerticesBasedOnEdgeValuesTest(false /* use min to aggregate edges */)
  }
  
  private def doUpdateVerticesBasedOnEdgeValuesTest(useMaxAggr: Boolean) {
    val sc = new SparkContext("local", "updateVerticesBasedOnEdgeValuesTest")
    var g = GraphLoader.textFileWithVertexValues(sc, smallIntegerWeightedGraphVerticesFileName,
      smallIntegerWeightedGraphEdgesFileName,
      (id, values) => -1, (srcDstId, evalsString) => evalsString(0).toInt)
    println("printing edges before calling updateVertices...")
    println(g.edges.collect().deep.mkString("\n"))
    println("finished printing edges before calling updateVertices...")
    g = g.updateVerticesBasedOnOutgoingEdgeValues((vertex, edgesList) => {
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
    g = g.mapVertices{ v => v.data.doubleValue = v.id.toDouble; v.data }
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

  private def doSimplePropagateInBothDirectionsFromOneTest(useReflection: Boolean, testName: String) {
   var (sc, g) = loadGraphFromVertexAndEdgeFileAndCache(testSmallNotConnectedGraphVertexFile,
	  testSmallNotConnectedGraphEdgeFile, testName)
    // Simply propagates 5's value (which is 7) around with max aggregation
    g = {
      if (useReflection) {
        g.simplePropagateInBothDirectionsFromOneReflection[Int](
          4 /* start vertex */,
          "intValue1",
          maxAggr)
      } else {
        g.simplePropagateInBothDirectionsFromOne[Int](
          4 /* start vertex */ ,
          v => v.intValue1,
          maxAggr,
          (vertexValue: DoubleIntInt, finalValue: Int) => {
            vertexValue.intValue1 = finalValue
            vertexValue
          })
      }
    }
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
      if (vertex.id == 7) assert(11 == vertex.data.intValue1)
      else assert(10 == vertex.data.intValue1)
    }
    sc.stop()
  }

  test("simplePropagateForwardFromAllTest") {
	doSimplePropagateForwardFromAllTest(false /* do not use reflection */,
	  "simplePropagateForwardFromAllTest")
  }

  test("simplePropagateForwardFromAllReflectionTest") {
	doSimplePropagateForwardFromAllTest(true /* use reflection */,
	  "simplePropagateForwardFromAllTest")
  }

  private def doSimplePropagateForwardFromAllTest(useReflection: Boolean, testName: String) {
    var (sc, g) = loadGraphFromVertexAndEdgeFileAndCache(testSmallNotConnectedGraphVertexFile,
	  testSmallNotConnectedGraphEdgeFile, testName)
    g = {
      if (useReflection) {
        g.simplePropagateForwardFromAllReflection[Int]("intValue1", maxAggr)
      } else {
        g.simplePropagateForwardFromAll[Int](
          v => v.intValue1,
          maxAggr,
          (vertexValue: DoubleIntInt, finalValue: Int) => {
            vertexValue.intValue1 = finalValue
            vertexValue
          })
      }
    }
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

  test("simplePropagateInTransposeFromAllTest") {
	  doSimplePropagateInTransposeFromAllTest(false /* do not use reflection */,
	    "simplePropagateInTransposeFromAllTest")
  }

  test("simplePropagateInTransposeFromAllReflectionTest") {
	  doSimplePropagateInTransposeFromAllTest(true /* use reflection */,
	    "simplePropagateInTransposeFromAllReflectionTest")
  }

  private def doSimplePropagateInTransposeFromAllTest(useReflection: Boolean, testName: String) {
    var (sc, g) = loadGraphFromVertexAndEdgeFileAndCache(testSmallNotConnectedGraphVertexFile,
	  testSmallNotConnectedGraphEdgeFile, testName)
    g = {
      if (useReflection) {
        g.simplePropagateInTransposeFromAllReflection[Int]("intValue1", maxAggr)
      } else {
        g.simplePropagateInTransposeFromAll[Int](
          v => v.intValue1,
          maxAggr,
          (vertexValue: DoubleIntInt, finalValue: Int) => {
            vertexValue.intValue1 = finalValue
            vertexValue
          })
      }
    }
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

  test("simplePropagateInBothDirectionsFromAllTest") {
    doSimplePropagateInBothDirectionsFromAllTest(false /* do not use reflection */,
      "simplePropagateInBothDirectionsFromAllTest")
  }

  test("simplePropagateInBothDirectionsFromAllReflectioTest") {
    doSimplePropagateInBothDirectionsFromAllTest(true /* use reflection */,
      "simplePropagateInBothDirectionsFromAllReflectionTest")
  }
  
  private def doSimplePropagateInBothDirectionsFromAllTest(useReflection: Boolean, testName: String) {
    	var (sc, g) = loadGraphFromVertexAndEdgeFileAndCache(testSmallNotConnectedGraphVertexFile,
	  testSmallNotConnectedGraphEdgeFile, testName)
    g = {
      if (useReflection) {
        g.simplePropagateInBothDirectionsFromAllReflection[Int]("intValue1", maxAggr)
      } else {
        g.simplePropagateInBothDirectionsFromAll[Int](
          v => v.intValue1,
          maxAggr,
          (vertexValue: DoubleIntInt, finalValue: Int) => {
            vertexValue.intValue1 = finalValue
            vertexValue
          })
      }
    }
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
      assert(11 == vertex.data.intValue1)
    }
    sc.stop()
  }

  private def doSimplePropagateTest(direction: EdgeDirection) = {
    var (sc, g) = loadGraphFromVertexAndEdgeFileAndCache(testSmallGraphVertexFile, testSmallGraphEdgeFile,
      "simplePropagateTest")
    // Simply propagates 4 around with null aggregation
    g = g.simplePropagate(direction,
      v => v.id == 4,
      v => v.intValue1,
      (vertexField: Int, msgs: Seq[Int]) => { msgs(0) },
      (vertexValue, finalValue: Int) => {
        vertexValue.intValue1 = finalValue
        vertexValue
      })
    verifyThatValuesHaveConvergedForSimplePropagationTestAndStopContext(sc, g, direction)
  }

  private def checkIntValue1AndIntValue2AreUnchanged(vertex: Vertex[DoubleIntInt]) = {
    assert(-1 == vertex.data.intValue2)
    assert(((vertex.id + 2) % 8) == vertex.data.intValue1)
  }

  private def doPropagateUntilConvergenceTest(direction: EdgeDirection, useReflection: Boolean, testName: String) {
    var (sc, g) = loadGraphFromVertexAndEdgeFileAndCache(testSmallGraphVertexFile, testSmallGraphEdgeFile,
      testName)
    g = {
      if (useReflection) {
        g.propagateUntilConvergenceReflection[Double](direction,
          v => v.id == 0,
          "doubleValue",
          (propagatedValue, edgeValue) => propagatedValue + edgeValue,
          minAggr)
      } else {
        g.propagateUntilConvergence[Double](
          direction,
          v => v.id == 0,
          value => value.doubleValue,
          (propagatedValue, edgeValue) => propagatedValue + edgeValue,
          minAggr,
          (vertexValue, finalValue) => {
            if (finalValue < vertexValue.doubleValue) {
              println("finalValue: " + finalValue + " is less than vertex value: " + vertexValue.doubleValue)
              vertexValue.doubleValue = finalValue
              println("set vertexvalue double value: " + vertexValue.doubleValue)
            }
            println("returned vertexValue's doubleValue: " + vertexValue.doubleValue)
            vertexValue
          })
      }
    }
    verifyThatValuesHaveConvergedForPropagationTestAndStopContext(sc, g, direction)
  }

  private def doPropagateFixedNumberOfIterationsSevenAndLargerNumberOfIterationsTest(numIter: Int,
    direction: EdgeDirection, testName: String, isReflection: Boolean) {
    var (sc, g) = {
      if (isReflection) loadGraphAndRunPropagateFixNumberOfIterationsReflection(numIter, direction, testName)
      else loadGraphAndRunPropagateFixNumberOfIterations(numIter, direction, testName)
    }
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
    g = g.propagateFixedNumberOfIterations[Double](
      direction,
      v => v.id == 0,
      value => value.doubleValue,
      (propagatedValue, edgeValue) => propagatedValue + edgeValue,
      minAggr,
      (vertexValue, finalValue) => {
        if (finalValue < vertexValue.doubleValue) {
          println("finalValue: " + finalValue + " is less than vertex value: " + vertexValue.doubleValue)
          vertexValue.doubleValue = finalValue
          println("set vertexvalue double value: " + vertexValue.doubleValue)
        }
        println("returned vertexValue's doubleValue: " + vertexValue.doubleValue)
        vertexValue
      },
      numIter)
    (sc, g)
  }
  
  private def loadGraphAndRunPropagateFixNumberOfIterationsReflection(numIter: Int, direction: EdgeDirection,
    testName: String) = {
    var (sc, g) = loadGraphFromVertexAndEdgeFileAndCache(testSmallGraphVertexFile, testSmallGraphEdgeFile,
      testName)
    g = g.propagateFixedNumberOfIterationsReflection[Double](
      direction,
      v => v.id == 0,
      "doubleValue",
      (propagatedValue, edgeValue) => propagatedValue + edgeValue,
      (vertexValue, msgs) => {
        var minValue = vertexValue
        println("starting minValue: " + minValue + " msgs: " + msgs)
        for (msg <- msgs) {
          println("msg: " + msg)
          print("min of minValue: " + minValue + " msg: " + msg + " is: ")
          minValue = scala.math.min(minValue, msg)
          println("" + minValue)
        }
        println("returning: " + minValue)
        minValue
      },
      numIter)
    (sc, g)
  }

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
