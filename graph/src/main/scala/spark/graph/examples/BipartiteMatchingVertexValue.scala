package spark.graph.examples

class BipartiteMatchingVertexValue(var tmpPick: Int, var actualPick: Int) extends Serializable {
	override def toString = { "tmpPick: " + tmpPick  + " actualPick: " + actualPick }
}