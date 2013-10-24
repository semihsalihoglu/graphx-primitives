package spark.graph.examples

class MetisVertexValue(var weight: Double, var superVertexID: Int, var partitionID: Int,
  var tentativePartitionID: Int) extends Serializable {
	override def toString = { "weight: " + weight + " superVertexID: " + superVertexID +
	  " partitionID: " + partitionID + " tentativePartitionID: " + tentativePartitionID}
}
