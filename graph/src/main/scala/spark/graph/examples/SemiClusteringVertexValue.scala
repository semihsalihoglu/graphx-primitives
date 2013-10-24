package spark.graph.examples

import spark.graph.Vid

class SemiClusteringVertexValue(var semiClusters: List[(Double, Double, List[Vid])]) extends Serializable {
	override def toString = { "semiClusters: " + semiClusters}
}