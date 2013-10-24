package spark.graph.impl

import scala.collection.JavaConversions._
import scala.util.Random
import spark.{ ClosureCleaner, HashPartitioner, RDD }
import spark.SparkContext._
import spark.graph._
import spark.graph.impl.GraphImpl._
import spark.graph.examples.DoubleIntInt
import scala.collection.mutable.ListBuffer
import spark.graph.examples.ExampleAlgorithms

class GraphImplWithPrimitives[VD: ClassManifest, ED: ClassManifest] protected (
  override val numVertexPartitions: Int,
  override val numEdgePartitions: Int,
  _rawVertices: RDD[Vertex[VD]],
  _rawEdges: RDD[Edge[ED]],
  _rawVTable: RDD[(Vid, (VD, Array[Pid]))],
  _rawETable: RDD[(Pid, EdgePartition[ED])])
  extends GraphImpl[VD, ED](numVertexPartitions,
    numEdgePartitions: Int,
    _rawVertices,
    _rawEdges,
    _rawVTable,
    _rawETable) {

  def this(vertices: RDD[Vertex[VD]], edges: RDD[Edge[ED]]) = {
    this(vertices.partitions.size, edges.partitions.size, vertices, edges, null, null)
  }

  override def newGraph[VD2: ClassManifest, ED2: ClassManifest](
    vertices: RDD[Vertex[VD2]], edges: RDD[Edge[ED2]]): Graph[VD2, ED2] = {
    (new GraphImplWithPrimitives[VD2, ED2](vertices, edges))
  }

  override def newGraph[VD: ClassManifest, ED: ClassManifest](numVertexPartitions: Int,
    numEdgePartitions: Int,
    _rawVertices: RDD[Vertex[VD]],
    _rawEdges: RDD[Edge[ED]],
    _rawVTable: RDD[(Vid, (VD, Array[Pid]))],
    _rawETable: RDD[(Pid, EdgePartition[ED])]): Graph[VD, ED] = {
    new GraphImplWithPrimitives(numVertexPartitions, numEdgePartitions, _rawVertices, _rawEdges,
      _rawVTable, _rawETable)
  }

  override def filterEdges(p: Edge[ED] => Boolean): Graph[VD, ED] = {
    newGraph(vertices, edges.filter(p))
  }

  override def filterEdgesBasedOnSourceDestAndValue(p: EdgeTriplet[VD, ED] => Boolean): Graph[VD, ED] = {
    println("triplets count: " + triplets.count)
    val filteredEdgeTriplets = triplets.filter(p)
    println("filtered triplets count: " + filteredEdgeTriplets.count)
    val filteredEdges = filteredEdgeTriplets.map(triplet => new Edge(triplet.src.id, triplet.dst.id,
      triplet.data))
    println("filtered edges count: " + filteredEdges.count)
    newGraph(vertices, filteredEdges)
  }

  override def filterVertices(p: Vertex[VD] => Boolean): Graph[VD, ED] = {
    val tmpGraph = newGraph(vertices.filter(p), edges)
    tmpGraph.correctEdges()
  }

  override def filterVerticesUsingLocalEdges(
    direction: EdgeDirection,
    p: ((Vid, (VD, Option[Seq[Edge[ED]]]))) => Boolean): Graph[VD, ED] = {
    val joinedVertexAndNeighbors = getJoinedVertexAndNeighbors(direction)
    val filteredJoinedVertexAndNeighbors = joinedVertexAndNeighbors.filter(p);
    val newVertices = filteredJoinedVertexAndNeighbors.map(
      vIdVertexAndNeighbors => new Vertex(vIdVertexAndNeighbors._1, vIdVertexAndNeighbors._2._1))
    //	println(newVertices.collect().deep.mkString("\n"))
    val tmpGraph = newGraph(newVertices, edges)
    val correctedGraph = tmpGraph.correctEdges()
    correctedGraph
  }

  override def aggregateNeighborValuesFixedNumberOfIterations[A](
    direction: EdgeDirection,
    startVF: Vertex[VD] => Boolean,
    msgF: Vertex[VD] => Option[A],
    propagateAlongEdgeF: (A, ED) => A,
    updateF: ((Vertex[VD], Seq[Edge[ED]]), Option[Seq[A]]) => VD,
    numIter: Int) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
    var individualMsgs = vertices.map { v => (v.id, msgF(v)) }
    def computeFromNbrIDToNbrIDTable(d: EdgeDirection): RDD[(Vid, Vid)] = {
      // if propagating through in neighborse we will be joining the messages with e.srcId
      if (EdgeDirection.In == d) { edges.flatMap { e => List((e.src, e.dst)) } }
      else if (EdgeDirection.Out == d) { edges.flatMap { e => List((e.dst, e.src)) } }
      else { edges.flatMap { e => List((e.dst, e.src), (e.src, e.dst)) } }
    }
    val fromNbrIDToNbrID = computeFromNbrIDToNbrIDTable(direction)
    val fromNbrIDToNbrIDMsg = fromNbrIDToNbrID.join(individualMsgs)
    val toNbrIDMsgs = fromNbrIDToNbrIDMsg.flatMap { fromIDToIDOptionalMsg => {
      if (fromIDToIDOptionalMsg._2._2.isDefined) { List((fromIDToIDOptionalMsg._2._1, fromIDToIDOptionalMsg._2._2.get))}
      else {None}
    }}.groupByKey()
    val vertexIDVertex = vertices.map { v => (v.id, v) }
    def computeNbrIDEdgeTable(d: EdgeDirection): RDD[(Vid, Edge[ED])] = {
      // if propagating through in neighborse we will be joining the messages with e.srcId
      if (EdgeDirection.In == d) { edges.flatMap { e => List((e.dst, e)) } }
      else if (EdgeDirection.Out == d) { edges.flatMap { e => List((e.src, e)) } }
      else { edges.flatMap { e => List((e.src, e), (e.dst, e)) } }
    }
    val vertexIDEdges = computeNbrIDEdgeTable(direction).groupByKey
    val vertexIDVertexEdges = vertexIDVertex.leftOuterJoin(vertexIDEdges)
    val vertexIDVertexEdgesMsgsTable = vertexIDVertexEdges.leftOuterJoin(toNbrIDMsgs)
    val newVertices = vertexIDVertexEdgesMsgsTable.map{ vertexIDVertexEdgesMsgs =>
      val vertex = vertexIDVertexEdgesMsgs._2._1._1
      if (!startVF(vertex)) {
        vertex
      } else {
        var edges: Seq[Edge[ED]] = List()
        if (vertexIDVertexEdgesMsgs._2._1._2.isDefined) {
          edges = vertexIDVertexEdgesMsgs._2._1._2.get
        }
        new Vertex(vertex.id, updateF(
          (vertexIDVertexEdgesMsgs._2._1._1, edges), vertexIDVertexEdgesMsgs._2._2))
      }      
    }
    newGraph(newVertices, edges)
  }

  override def updateVerticesBasedOnNeighbors[A](
	direction: EdgeDirection,
    startVF: Vertex[VD] => Boolean,
    msgF: Vertex[VD] => Option[A],
    updateF: (Vertex[VD], Option[Seq[A]]) => VD) (implicit m: Manifest[VD], n:Manifest[A]): Graph[VD, ED] = {
    aggregateNeighborValuesFixedNumberOfIterations[A](direction, startVF, msgF, (msg, edgeValue) => msg,
      (vertexAndnbrIDs, msgs) => updateF(vertexAndnbrIDs._1, msgs),
      1 /* one iteration */)
  }

  override def updateVerticesUsingLocalEdges[VD2: ClassManifest](
    direction: EdgeDirection,
    map: (Vertex[VD], Option[Seq[Edge[ED]]]) => VD2): Graph[VD2, ED] = {
    val joinedVertexAndNeighbors = getJoinedVertexAndNeighbors(direction)
    // TODO(semih): calling persist here because in the GC algorithm this somehow gets computed 3 times
    val newVertices = joinedVertexAndNeighbors.map(vIdVertexAndNeighbors => 
      new Vertex(vIdVertexAndNeighbors._1, map(
        new Vertex[VD](vIdVertexAndNeighbors._1, vIdVertexAndNeighbors._2._1), vIdVertexAndNeighbors._2._2))).persist()
    val tmpGraph = newGraph(newVertices, edges)
    val correctedGraph = tmpGraph.correctEdges()
    correctedGraph
  }

  override def aggregateGlobalValueOverVertices[A](direction: EdgeDirection,
    mapF: ((Vid, (VD, Option[Seq[spark.graph.Edge[ED]]]))) => Iterable[A],
    reduceF: (A, A) => A) (implicit m: Manifest[VD], n: Manifest[A]): A = {
    val joinedVertexAndNeighbors = getJoinedVertexAndNeighbors(direction)
    joinedVertexAndNeighbors.flatMap[A](mapF).reduce(reduceF)
  }

  def getJoinedVertexAndNeighbors(direction: EdgeDirection): RDD[(Vid, (VD, Option[Seq[spark.graph.Edge[ED]]]))] = {
      val neighborIDEdgeValue = edges.flatMap(e =>
      direction match {
        case EdgeDirection.Out =>
          List((e.src, e))
        case EdgeDirection.In =>
          List((e.dst, e))
        case EdgeDirection.Both =>
          List((e.src, e), (e.dst, e))
      }).groupByKey()
    val vertexIDVertexValue = vertices.map { v => (v.id, v.data) }
    vertexIDVertexValue.leftOuterJoin(neighborIDEdgeValue)
  }

  override def updateAnotherVertexBasedOnSelf[A](
    fromVertexF: Vertex[VD] => Boolean,
    idF: Vertex[VD] => Vid,
    msgF: Vertex[VD] => A,
    setF: (VD, Seq[A]) => VD) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
    val neighborIDMsgsTable = vertices.flatMap { 
    	  vertex => if (fromVertexF(vertex)) { Some(idF(vertex), msgF(vertex)) } else None }.groupByKey
    println(neighborIDMsgsTable.collect().deep.mkString("\n"))
    val vertexIDVertexValue = vertices.map { v => (v.id, v) }
    val idVertexValueMsgs = vertexIDVertexValue.leftOuterJoin(neighborIDMsgsTable)
    val newVertices = idVertexValueMsgs.map(
      idVertexValueMsgs => 
        // If there are no messages then the vertex stays as is
        if (!idVertexValueMsgs._2._2.isDefined) {idVertexValueMsgs._2._1}
        // Otherwise we apply the setF to update the vertex value
        else {
        (new Vertex(idVertexValueMsgs._2._1.id,
        setF(idVertexValueMsgs._2._1.data, idVertexValueMsgs._2._2.get))) })
    println ("printing new vertices ....")
    println(newVertices.collect().deep.mkString("\n"))
    newGraph(newVertices, edges)
  }

  override def updateSelfUsingAnotherVertexsValue[A](
    verticesToUpdateP: Vertex[VD] => Boolean,
    idF: Vertex[VD] => Vid,
    msgF: Vertex[VD] => A,
    setF: (Vertex[VD], A) => VD) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
    //    ClosureCleaner.clean(setF)
    val neighborIDVertexTable = vertices.map { vertex => (idF(vertex), vertex) }
    println(neighborIDVertexTable.collect().deep.mkString("\n"))
    val idNeighborValueVertexValue = computeVertexIdVertexDataTableAndJoinWithNbrIdVertexTable(neighborIDVertexTable)
    val newVertices = idNeighborValueVertexValue.map(
      neighborIDNeighborValueVertex => (new Vertex(neighborIDNeighborValueVertex._2._2.id,
        setF(neighborIDNeighborValueVertex._2._2,
          msgF(neighborIDNeighborValueVertex._2._1)))))
    newGraph(newVertices, edges)
  }

  override def pickRandomVertices(p: Vertex[VD] => Boolean, numVerticesToPick: Int): Seq[Vid] = {
    val numVerticesToPickFrom = vertices.filter(p).count()
    val actualNumVerticesToPick = scala.math.min(numVerticesToPickFrom, numVerticesToPick).intValue()
    val probability = 50*actualNumVerticesToPick / numVerticesToPickFrom
    var found = false
    var retVal: ListBuffer[Vid] = ListBuffer()
    while (!found) {
      val selectedVertices = vertices.flatMap { v =>
        if (p(v) && Random.nextDouble() < probability) { Some(v.id) }
        else { None }
      }
      var collectedSelectedVertices = ListBuffer[Vid]()
      collectedSelectedVertices.appendAll(selectedVertices.collect())
      println("collectedSelectedVertices: " + collectedSelectedVertices)
      if (collectedSelectedVertices.size >= actualNumVerticesToPick) {
        found = true
        for (i <- 1 to actualNumVerticesToPick) {
          val randomIndex = Random.nextInt(collectedSelectedVertices.size)
          retVal.add(collectedSelectedVertices(randomIndex))
          collectedSelectedVertices.remove(randomIndex)
        }
      } else {
        println("COULD NOT PICK A VERTEX. TRYING AGAIN....")
      }
    }
    retVal
  }

  override def propagateAndAggregateFixedNumberOfIterations[A](
    direction: EdgeDirection,
    startVF: Vertex[VD] => Boolean,
    propagatedFieldF: VD => A,
    propagateAlongEdgeF: (A, ED) => A,
//    aggrF: (A, Seq[A]) => A,
    setF: (Vertex[VD], Seq[A]) => VD,
    numIter: Int) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
    doAggregateNeighborsFixedNumberOfIterations(direction, startVF, propagatedFieldF, propagateAlongEdgeF,
      /* aggrF, */ setF, numIter, false /* propagate only the changed vertices */)
  }

  private def doAggregateNeighborsFixedNumberOfIterations[A](
    direction: EdgeDirection,
    startVF: Vertex[VD] => Boolean,
    propagatedFieldF: VD => A,
    propagateAlongEdgeF: (A, ED) => A,
//    aggrF: (A, Seq[A]) => A,
    setF: (Vertex[VD], Seq[A]) => VD,
    numIter: Int,
    aggregateAll: Boolean) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
    assert(numIter > 0)
    println("running propagateFixedNumberOfIterations")
    var (extendedVertices, neighborIDEdgeValue) = computeExtendedVerticesAndNeighborIDEdgeValue(direction,
      startVF)
    for (i <- 0 until numIter) yield {
      extendedVertices = runOneIterationOfPropagation(propagatedFieldF, propagateAlongEdgeF,
        /* aggrF, */ setF,
        extendedVertices, neighborIDEdgeValue, aggregateAll)
    }
    val newVertices = extendedVertices.map { v => new Vertex(v._1, v._2.value) }
    newGraph(newVertices, edges)
  }
 
  override def propagateAndAggregateUntilConvergence[A](
    direction: EdgeDirection,
    startVF: Vertex[VD] => Boolean,
    propagatedFieldF: VD => A,
    propagateAlongEdgeF: (A, ED) => A,
    setF: (Vertex[VD], Seq[A]) => VD) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
    var (extendedVertices, neighborIDEdgeValue) = computeExtendedVerticesAndNeighborIDEdgeValue(direction,
      startVF)
    ExampleAlgorithms.debugRDD(extendedVertices.collect, "initial extended vertices")
    var numDiff = computeNumDiff(extendedVertices)
    var iterNo = 0
    while (numDiff > 0) {
      iterNo += 1
      println("iterationNumber: " + iterNo)
      extendedVertices = runOneIterationOfPropagation(propagatedFieldF, propagateAlongEdgeF, /* aggrF,*/ setF,
      extendedVertices, neighborIDEdgeValue, false /* do not start from all */)
      numDiff = computeNumDiff(extendedVertices)
    }
    val newVertices = extendedVertices.map { v => new Vertex(v._1, v._2.value) }
    newGraph(newVertices, edges)
  }

  // Warning!! We only handle the case when groupByKeyF is of type Int
  override def formSuperVertices(groupByKeyF: VD => Vid, edgeAggrF: Seq[ED] => ED, vertexAggrF: Seq[VD] => VD,
    removeSelfLoops: Boolean) (implicit m: Manifest[VD]): Graph[VD, ED] = {
    var newVerticesWithOnlyNewIDData = vertices.map { v => (new Vertex[Int](v.id, groupByKeyF(v.data))) }
    val tmpGraphToUseEdgeTriplets = newGraph(newVerticesWithOnlyNewIDData, edges)
    val newSrcIDDestIDEdgeValues = tmpGraphToUseEdgeTriplets.triplets.flatMap { triplet =>
      val newSrcId = triplet.src.data
      val newDstId = triplet.dst.data
      if (removeSelfLoops && (newSrcId == newDstId)) {
        println("removing self loop: oldIDs(" + triplet.src.id + ", " + triplet.dst.id + ") newIDs:(" +
          newSrcId + ", " + newDstId + ")")
        None
      } else {
        Some(((triplet.src.data, triplet.dst.data), triplet.data))
      }
    }
    val newSrcIDDestIDGroupedValues = newSrcIDDestIDEdgeValues.groupByKey()
    val newEdges = newSrcIDDestIDGroupedValues.map{ newSrcIdDstIdGroupedValue =>
      new Edge(newSrcIdDstIdGroupedValue._1._1, newSrcIdDstIdGroupedValue._1._2, edgeAggrF(newSrcIdDstIdGroupedValue._2)) }
    val newVertexIdGroupedByVertexValues = vertices.map { v => (groupByKeyF(v.data), v.data) }.groupByKey()
    val newVertices = newVertexIdGroupedByVertexValues.map(
      newIdSeqVertexValues => new Vertex[VD](newIdSeqVertexValues._1, vertexAggrF(newIdSeqVertexValues._2)))
    newGraph(newVertices, newEdges)
  }

  private def computeNumDiff(extendedVertices: RDD[(Vid, VertexValueChanged[VD])]): Long = {
     extendedVertices.flatMap{x => 
        if (x._2.changed) { Some(1) }
        else { None }
      }.count()
  }

  private def computeExtendedVerticesAndNeighborIDEdgeValue(direction: EdgeDirection,
    startVF: Vertex[VD] => Boolean): (RDD[(Vid, VertexValueChanged[VD])], RDD[(Vid, (Vid, ED))]) = {
    var extendedVertices = vertices.map { v => (v.id, new VertexValueChanged(v.data, startVF(v))) }
    def computeNeighborIdEdgeValueTable(d: EdgeDirection): RDD[(Vid, (Vid, ED))] = {
      // if propagating through in neighborse we will be joining the messages with e.srcId
      if (EdgeDirection.In == d) { edges.flatMap { e => List((e.dst, (e.src, e.data))) } }
      else if (EdgeDirection.Out == d) { edges.flatMap { e => List((e.src, (e.dst, e.data))) } }
      else { edges.flatMap { e => List((e.src, (e.dst, e.data)), (e.dst, (e.src, e.data))) } }
    }
    val neighborIDEdgeValue = computeNeighborIdEdgeValueTable(direction)
    (extendedVertices, neighborIDEdgeValue)
  }

  private def computeVertexIdVertexDataTableAndJoinWithNbrIdVertexTable(
    neighborIDVertexTable: RDD[(Vid, Vertex[VD])]): spark.RDD[(Vid, (Vertex[VD], Vertex[VD]))] = {
    val vertexIDVertexValue = vertices.map { v => (v.id, v) }
    val idNeighborValueVertexValue = vertexIDVertexValue.join(neighborIDVertexTable)
    idNeighborValueVertexValue
  }

  private def runOneIterationOfPropagation[A](propagatedFieldF: VD => A, propagateAlongEdgeF: (A, ED) => A,
    /* aggrF: (A, Seq[A]) => A, */ setF: (Vertex[VD], Seq[A]) => VD, extendedVertices: spark.RDD[(Vid, VertexValueChanged[VD])],
    neighborIDEdgeValue: spark.RDD[(Vid, (Vid, ED))],
    startFromAllEachIteration: Boolean) (implicit m: Manifest[VD], n: Manifest[A]): RDD[(Vid, VertexValueChanged[VD])] = {
    // We compute the messages by:
    // (1) joining the fields of each vertex v with their neighbors w they should send a message to
    // (2) then executing the propagateAlongEdgeF function on the (v.data, (v, w).data).
    val msgs = extendedVertices.join(neighborIDEdgeValue).flatMap {
      fieldNbrIdEdgeValue =>
        if (fieldNbrIdEdgeValue._2._1.changed) {
          Some((fieldNbrIdEdgeValue._2._2._1,
            propagateAlongEdgeF(propagatedFieldF(fieldNbrIdEdgeValue._2._1.value),
              fieldNbrIdEdgeValue._2._2._2)))
        } else {
          None
        }
    }.groupByKey()
    println("printing msgs...")
    println(msgs.collect().deep.mkString("\n"))

    val vertexIdValue = extendedVertices.map { v => (v._1, v._2.value) }
    var newExtendedVertices = vertexIdValue.leftOuterJoin(msgs).map { vIDVertexDataMsgs =>
      val vertexID = vIDVertexDataMsgs._1
      val vertexData = vIDVertexDataMsgs._2._1
      val msgs = vIDVertexDataMsgs._2._2
      if (msgs.isEmpty || !msgs.isDefined) {
        println("No messages for vertex: " + vertexID)
        (vertexID, new VertexValueChanged(vertexData, false || startFromAllEachIteration))
      } else {
        println("There are messages for vertex: " + vertexID + " msgs: " + msgs.get)
        val oldData = vertexData
        val propagatingField = propagatedFieldF(vertexData)
        val oldValue = propagatedFieldF(oldData)
        val newData = setF(Vertex(vertexID, vertexData), msgs.get)
        val changed = startFromAllEachIteration || oldValue != propagatedFieldF(newData)
        println("changed: " + changed + " oldField: " + oldValue + " newField: "
          + propagatedFieldF(newData))
        (vertexID, new VertexValueChanged(newData, changed))
      }
    }
    println("printing new extendedVertices...")
    println(newExtendedVertices.collect().deep.mkString("\n"))
    newExtendedVertices
  }
}