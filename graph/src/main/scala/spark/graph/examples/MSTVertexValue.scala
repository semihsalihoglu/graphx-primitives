package spark.graph.examples

import spark.graph.Vid

class MSTVertexValue (var pickedNbrId: Vid, var pickedEdgeOriginalSrcId: Vid, var pickedEdgeOriginalDstId: Vid,
  var pickedEdgeWeight: Double)
	extends Serializable {

	override def toString = { "pickedNbrId: " + pickedNbrId + " pickedEdgeOriginalSrcId: " +
	  pickedEdgeOriginalSrcId + " pickedEdgeOriginalDstId: " + pickedEdgeOriginalDstId + " pickedEdgeWeight: " + pickedEdgeWeight}
}