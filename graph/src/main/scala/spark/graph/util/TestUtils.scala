package spark.graph.util

import scala.io.Source
import java.io.PrintWriter
import java.io.File

object TestUtils {

  def main(args: Array[String]) {
      var edges = Map[(Int, Int), Double]()
      var vertices = Set[Int]()
	  for(line <- Source.fromFile("/Users/semihsalihoglu/tmp/mediumEWG_gps.txt").getLines()) {
		val lineArray = line.split("\\s+")
		val srcId = lineArray(0).trim.toInt
		vertices += srcId
		var index = 1
		while (index < (lineArray.length - 1)) {
		  val srcIdDestIdTuple = ((srcId, lineArray(index).trim.toInt), lineArray(index + 1).trim.toDouble)
		  edges += srcIdDestIdTuple
		  index += 2
		}
	  }

	  val verticesFile = new PrintWriter(new File("/Users/semihsalihoglu/projects/graphx/spark/test-data/mediumEWG_vertices.txt" ))
      for (v <- vertices) {
        verticesFile.write(v + "\n")
      }
      verticesFile.close()

      val edgesFile = new PrintWriter(new File("/Users/semihsalihoglu/projects/graphx/spark/test-data/mediumEWG_edges.txt" ))
      for (e <- edges) {
        edgesFile.write(e._1._1 + " " + e._1._2 + " " + e._2 + "\n")
      }
      edgesFile.close()
  }
}