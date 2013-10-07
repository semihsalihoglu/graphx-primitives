package spark.graph.examples

class DoubleIntInt (var doubleValue: Double, var intValue1: Int, var intValue2: Int) extends Serializable {

	override def toString = { "doubleValue: " + doubleValue + " intValue1: " + intValue1 + " intValue2: " + intValue2}
}
