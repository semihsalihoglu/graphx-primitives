package spark.graph.impl

class VertexValueChanged[VD](var value: VD, var changed: Boolean) extends Serializable {
  override def toString = { "value: " + value + " changed: " + changed }
}