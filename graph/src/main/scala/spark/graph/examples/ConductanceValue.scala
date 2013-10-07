package spark.graph.examples

class ConductanceValue(var inSubset: Boolean, var count: Int) extends Serializable {
	override def toString = { "inSubset: " + inSubset }
}