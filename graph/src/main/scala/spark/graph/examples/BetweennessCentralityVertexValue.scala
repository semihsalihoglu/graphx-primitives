package spark.graph.examples

class BetweennessCentralityVertexValue(var bfsLevel: Int, var sigma: Double, var delta: Double, var bc: Double) extends Serializable {
	override def toString = { "bfsLevel: " + bfsLevel  + " sigma: " + sigma + " delta: " + delta + " bc: " + bc }
}