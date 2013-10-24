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

@RunWith(classOf[JUnitRunner])
class HigherLevelComputationsTest  extends FunSuite with Serializable {
  var numVPart = 4
  var numEPart = 4
  var host = "local"
  var filterPrimitivesTestFile = "/Users/semihsalihoglu/projects/graphx/spark/test-data/test_filter_primitives.txt"

  test("pointerJumpingTest") {
    val sc = new SparkContext(host, "updateVertexValueBasedOnAnotherVertexsValueReflectionTest")
    val vertexValuesPath = "/Users/semihsalihoglu/projects/graphx/spark/test-data/test_pointer_jumping_vertices.txt"
    val edgesPath = "/Users/semihsalihoglu/projects/graphx/spark/test-data/test_pointer_jumping_edges.txt"
    var g = GraphLoader.textFileWithVertexValues(sc, vertexValuesPath, edgesPath, 
      (id, values) => new DoubleIntInt(values(0).trim().toDouble, values(1).trim().toInt, -1),
       (srcDstId, evalsString) => evalsString(0).toDouble)
    g = HigherLevelComputations.pointerJumping(g, v => v.data.intValue1,
      (v, msg) => { v.data.intValue1 = msg; v.data })   
    assert(8 == g.numEdges)
    assert(8 == g.numVertices)
    val localVertices = g.vertices.collect()
    for (vertex <- localVertices) {
      assert(-1 == vertex.data.intValue2)
      assert(vertex.data.intValue1 == 0)
    }
    sc.stop()
  }

  private def loadGraphAndCache(fname: String, testName: String):
	  (spark.SparkContext, spark.graph.Graph[Int,Double]) = {
    val sc = new SparkContext(host, testName)
    var g = GraphLoader.textFile(sc, fname, a => a(0).toDouble).withPartitioner(numVPart, numEPart).cache()
    (sc, g)
  }

  private def assertNumVerticesAndEdgesAndStop(sc: spark.SparkContext, g: spark.graph.Graph[Int,Double],
    expectedNumVertices: Int, expectedNumEdges: Int): Unit = {
    val numVertices = g.numVertices
    val numEdges = g.numEdges
    println("After: numVertices: " + numVertices + " numEdges: " + numEdges)
    assert(expectedNumVertices == numVertices)
    assert(expectedNumEdges == numEdges)
    sc.stop()
  }
}

