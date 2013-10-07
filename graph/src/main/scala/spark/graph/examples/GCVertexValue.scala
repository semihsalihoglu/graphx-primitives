package spark.graph.examples

class GCVertexValue(var vertexID: Int, var setType: MISType) extends Serializable {
	override def toString = { "vertexID: " + vertexID + " inMIS: " + setType }
}
