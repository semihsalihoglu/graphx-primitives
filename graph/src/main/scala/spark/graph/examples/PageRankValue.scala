package spark.graph.examples

class PageRankValue (var pageRank: Double) extends Serializable {
	override def toString = { "pageRank: " + pageRank}
}