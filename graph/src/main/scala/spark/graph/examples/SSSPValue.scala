package spark.graph.examples

class SSSPValue (var distance: Int) extends Serializable {
	override def toString = { "distance: " + distance}
}