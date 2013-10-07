package spark.graph.impl

import spark.graph.Vid

class VertexValueNewID[VD](var value: VD, var newID: Vid) extends Serializable {
  override def toString = { "value: " + value + " newID: " + newID }
}