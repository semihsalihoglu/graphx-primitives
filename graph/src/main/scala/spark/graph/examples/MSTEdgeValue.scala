package spark.graph.examples

class MSTEdgeValue (var weight: Double, var originalSrcId: Int, var originalDstId: Int) extends Serializable {

	override def toString = { "weight: " + weight + " originalSrcId: " + originalSrcId + " originalDstId: " + originalDstId}
}