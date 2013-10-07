package spark.graph.examples

class SCCValue (var colorID: Int, var expectedBWIdFoundPair: (Int, Boolean)) extends Serializable {

	override def toString = { "colorID: " + colorID + " expectedBWIdFoundPair: " + expectedBWIdFoundPair}
}
