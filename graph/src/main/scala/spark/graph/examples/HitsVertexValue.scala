package spark.graph.examples

class HitsVertexValue(var hubs: Double, var authority: Double) extends Serializable {
	override def toString = { "hubs: " + hubs + " authority: " + authority }
}