package spark.graph.examples
  
class WCCValue (var wccID: Int) extends Serializable {
	override def toString = { "wccID: " + wccID}
}
