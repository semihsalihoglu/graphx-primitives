package spark.graph.impl

import spark.{ ClosureCleaner, HashPartitioner, RDD }
import spark.SparkContext._

import spark.graph._
import spark.graph.impl.GraphImpl._

object HigherLevelComputations {

  /**
   * This is the pointer jumping algorithm used in Boruvka's MST algorithm and also in METIS algorithm.
   * (coarsening stage). Initially each vertex has a field called the pointerField that contains the id
   * of another vertex (call it its parent), which can possibly be itself
   * (actually some vertices should  point to themselves for convergence). Then vertices in iterations keep
   * updating their parents to point to their latest "parent's" parent until the pointers converge. In that
   * sense, this is a transitive closure-like algorithm. Both in MST and METIS after this operation all
   * vertices that point to the same vertex by forming super-vertex formation.
   */
  def pointerJumping[VD: Manifest, ED: Manifest](g: Graph[VD, ED], pointerFieldF: Vertex[VD] => Int,
    setF: (Vertex[VD], Int) => VD): Graph[VD, ED] = {
    var numDiff = Long.MaxValue
    var oldG = g
    while (numDiff > 0) {
      var newG = oldG.updateSelfUsingAnotherVertexsValue[Int](
        v => true,
        pointerFieldF,
        pointerFieldF,
        (v, nbrMsg) => setF(v, nbrMsg))
      val keyValueOldVertices = oldG.vertices.map { v => (v.id, v.data) }
      val keyValueNewVertices = newG.vertices.map { v => (v.id, v.data) }
      val oldNewVerticesJoined = keyValueOldVertices.join(keyValueNewVertices)
      val numDiffRDD = oldNewVerticesJoined.flatMap { idOldNewVertex =>
        val oldID = pointerFieldF(Vertex(idOldNewVertex._1, idOldNewVertex._2._1))
        val newID = pointerFieldF(Vertex(idOldNewVertex._1, idOldNewVertex._2._2))
        if (!oldID.equals(newID)) { Some(1) }
        else { None }
      }
      numDiff = numDiffRDD.count()
      oldG = newG
    }
    oldG
  }
}