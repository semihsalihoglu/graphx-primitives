package spark.graph.impl

import scala.collection.JavaConversions._

import spark.{ClosureCleaner, HashPartitioner, RDD}
import spark.SparkContext._

import spark.graph._
import spark.graph.impl.GraphImpl._


/**
 * A Graph RDD that supports computation on graphs.
 */
class GraphImpl[VD: ClassManifest, ED: ClassManifest] protected (
    val numVertexPartitions: Int,
    val numEdgePartitions: Int,
    _rawVertices: RDD[Vertex[VD]],
    _rawEdges: RDD[Edge[ED]],
    _rawVTable: RDD[(Vid, (VD, Array[Pid]))],
    _rawETable: RDD[(Pid, EdgePartition[ED])])
  extends Graph[VD, ED] {

  def this(vertices: RDD[Vertex[VD]], edges: RDD[Edge[ED]]) = {
    this(vertices.partitions.size, edges.partitions.size, vertices, edges, null, null)
  }

  def this () = this(-1, -1, null, null, null, null)

  def newGraph[VD: ClassManifest, ED: ClassManifest](numVertexPartitions: Int,
    numEdgePartitions: Int,
    _rawVertices: RDD[Vertex[VD]],
    _rawEdges: RDD[Edge[ED]],
    _rawVTable: RDD[(Vid, (VD, Array[Pid]))],
    _rawETable: RDD[(Pid, EdgePartition[ED])]) : Graph[VD, ED] = {
	  new GraphImpl(numVertexPartitions, numEdgePartitions, _rawVertices, _rawEdges, _rawVTable, _rawETable)
  }
    
  def withPartitioner(numVertexPartitions: Int, numEdgePartitions: Int): Graph[VD, ED] = {
    if (_cached) {
      newGraph(numVertexPartitions, numEdgePartitions, null, null, _rawVTable, _rawETable)
        .cache()
    } else {
      newGraph(numVertexPartitions, numEdgePartitions, _rawVertices, _rawEdges, null, null)
    }
  }

  def withVertexPartitioner(numVertexPartitions: Int) = {
    withPartitioner(numVertexPartitions, numEdgePartitions)
  }

  def withEdgePartitioner(numEdgePartitions: Int) = {
    withPartitioner(numVertexPartitions, numEdgePartitions)
  }

  protected var _cached = false

  override def cache(): Graph[VD, ED] = {
    eTable.cache()
    vTable.cache()
    _cached = true
    this
  }

  override def reverse: Graph[VD, ED] = {
    newGraph(vertices, edges.map{ case Edge(s, t, e) => Edge(t, s, e) })
  }

  /** Return a RDD of vertices. */
  override def vertices: RDD[Vertex[VD]] = {
    if (!_cached && _rawVertices != null) {
      _rawVertices
    } else {
      vTable.map { case(vid, (data, pids)) => new Vertex(vid, data) }
    }
  }

  /** Return a RDD of edges. */
  override def edges: RDD[Edge[ED]] = {
    if (!_cached && _rawEdges != null) {
      _rawEdges
    } else {
      eTable.mapPartitions { iter => iter.next()._2.iterator }
    }
  }

  /** Return a RDD that brings edges with its source and destination vertices together. */
  override def triplets: RDD[EdgeTriplet[VD, ED]] = {
    new EdgeTripletRDD(vTableReplicated, eTable).mapPartitions { part => part.next()._2 }
  }

  override def updateVertices[VD2: ClassManifest](f: Vertex[VD] => VD2): Graph[VD2, ED] = {
    newGraph(vertices.map(v => Vertex(v.id, f(v))), edges)
  }

  override def mapEdges[ED2: ClassManifest](f: Edge[ED] => ED2): Graph[VD, ED2] = {
    newGraph(vertices, edges.map(e => Edge(e.src, e.dst, f(e))))
  }

  override def mapTriplets[ED2: ClassManifest](f: EdgeTriplet[VD, ED] => ED2):
    Graph[VD, ED2] = {
    newGraph(vertices, triplets.map(e => Edge(e.src.id, e.dst.id, f(e))))
  }

  override def correctEdges(): Graph[VD, ED] = {
    val sc = vertices.context
    val vset = sc.broadcast(vertices.map(_.id).collect().toSet)
    val newEdges = edges.filter(e => vset.value.contains(e.src) && vset.value.contains(e.dst))
    Graph(vertices, newEdges)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // Lower level transformation methods
  //////////////////////////////////////////////////////////////////////////////////////////////////

  override def aggregateNeighbors[VD2: ClassManifest](
      mapFunc: (Vid, EdgeTriplet[VD, ED]) => Option[VD2],
      reduceFunc: (VD2, VD2) => VD2,
      default: VD2,
      gatherDirection: EdgeDirection)
    : RDD[(Vid, VD2)] = {

    ClosureCleaner.clean(mapFunc)
    ClosureCleaner.clean(reduceFunc)

    val newVTable = vTableReplicated.mapPartitions({ part =>
        part.map { v => (v._1, MutableTuple2(v._2, Option.empty[VD2])) }
      }, preservesPartitioning = true)

    new EdgeTripletRDD[MutableTuple2[VD, Option[VD2]], ED](newVTable, eTable)
      .mapPartitions { part =>
        val (vmap, edges) = part.next()
        val edgeSansAcc = new EdgeTriplet[VD, ED]()
        edgeSansAcc.src = new Vertex[VD]
        edgeSansAcc.dst = new Vertex[VD]
        edges.foreach { e: EdgeTriplet[MutableTuple2[VD, Option[VD2]], ED] =>
          edgeSansAcc.data = e.data
          edgeSansAcc.src.data = e.src.data._1
          edgeSansAcc.dst.data = e.dst.data._1
          edgeSansAcc.src.id = e.src.id
          edgeSansAcc.dst.id = e.dst.id
          if (gatherDirection == EdgeDirection.In || gatherDirection == EdgeDirection.Both) {
            e.dst.data._2 =
              if (e.dst.data._2.isEmpty) {
                mapFunc(edgeSansAcc.dst.id, edgeSansAcc)
              } else {
                val tmp = mapFunc(edgeSansAcc.dst.id, edgeSansAcc)
                if (!tmp.isEmpty) Some(reduceFunc(e.dst.data._2.get, tmp.get)) else e.dst.data._2
              }
          }
          if (gatherDirection == EdgeDirection.Out || gatherDirection == EdgeDirection.Both) {
            e.dst.data._2 =
              if (e.dst.data._2.isEmpty) {
                mapFunc(edgeSansAcc.src.id, edgeSansAcc)
              } else {
                val tmp = mapFunc(edgeSansAcc.src.id, edgeSansAcc)
                if (!tmp.isEmpty) Some(reduceFunc(e.src.data._2.get, tmp.get)) else e.src.data._2
              }
          }
        }
        vmap.int2ObjectEntrySet().fastIterator().filter(!_.getValue()._2.isEmpty).map{ entry =>
          (entry.getIntKey(), entry.getValue()._2)
        }
      }
      .map{ case (vid, aOpt) => (vid, aOpt.get) }
      .combineByKey((v: VD2) => v, reduceFunc, null, vertexPartitioner, false)
  }

  /**
   * Same as aggregateNeighbors but map function can return none and there is no default value.
   * As a consequence, the resulting table may be much smaller than the set of vertices.
   */
  override def aggregateNeighbors[VD2: ClassManifest](
    mapFunc: (Vid, EdgeTriplet[VD, ED]) => Option[VD2],
    reduceFunc: (VD2, VD2) => VD2,
    gatherDirection: EdgeDirection): RDD[(Vid, VD2)] = {

    ClosureCleaner.clean(mapFunc)
    ClosureCleaner.clean(reduceFunc)

    val newVTable = vTableReplicated.mapPartitions({ part =>
        part.map { v => (v._1, MutableTuple2(v._2, Option.empty[VD2])) }
      }, preservesPartitioning = true)

    new EdgeTripletRDD[MutableTuple2[VD, Option[VD2]], ED](newVTable, eTable)
      .mapPartitions { part =>
        val (vmap, edges) = part.next()
        val edgeSansAcc = new EdgeTriplet[VD, ED]()
        edgeSansAcc.src = new Vertex[VD]
        edgeSansAcc.dst = new Vertex[VD]
        edges.foreach { e: EdgeTriplet[MutableTuple2[VD, Option[VD2]], ED] =>
          edgeSansAcc.data = e.data
          edgeSansAcc.src.data = e.src.data._1
          edgeSansAcc.dst.data = e.dst.data._1
          edgeSansAcc.src.id = e.src.id
          edgeSansAcc.dst.id = e.dst.id
          if (gatherDirection == EdgeDirection.In || gatherDirection == EdgeDirection.Both) {
            e.dst.data._2 =
              if (e.dst.data._2.isEmpty) {
                mapFunc(edgeSansAcc.dst.id, edgeSansAcc)
              } else {
                val tmp = mapFunc(edgeSansAcc.dst.id, edgeSansAcc)
                if (!tmp.isEmpty) Some(reduceFunc(e.dst.data._2.get, tmp.get)) else e.dst.data._2
              }
          }
          if (gatherDirection == EdgeDirection.Out || gatherDirection == EdgeDirection.Both) {
            e.src.data._2 =
              if (e.src.data._2.isEmpty) {
                mapFunc(edgeSansAcc.src.id, edgeSansAcc)
              } else {
                val tmp = mapFunc(edgeSansAcc.src.id, edgeSansAcc)
                if (!tmp.isEmpty) Some(reduceFunc(e.src.data._2.get, tmp.get)) else e.src.data._2
              }
          }
        }
        vmap.int2ObjectEntrySet().fastIterator().filter(!_.getValue()._2.isEmpty).map{ entry =>
          (entry.getIntKey(), entry.getValue()._2)
        }
      }
      .map{ case (vid, aOpt) => (vid, aOpt.get) }
      .combineByKey((v: VD2) => v, reduceFunc, null, vertexPartitioner, false)
  }

  override def leftJoinVertices[U: ClassManifest, VD2: ClassManifest](
      updates: RDD[(Vid, U)],
      updateF: (Vertex[VD], Option[U]) => VD2)
    : Graph[VD2, ED] = {

    ClosureCleaner.clean(updateF)

    val newVTable = vTable.leftOuterJoin(updates).mapPartitions({ iter =>
      iter.map { case (vid, ((vdata, pids), update)) =>
        val newVdata = updateF(Vertex(vid, vdata), update)
        (vid, (newVdata, pids))
      }
    }, preservesPartitioning = true).cache()

    newGraph(newVTable.partitions.length, eTable.partitions.length, null, null, newVTable, eTable)
  }

  override def joinVertices[U: ClassManifest](
      updates: RDD[(Vid, U)],
      updateF: (Vertex[VD], U) => VD)
    : Graph[VD, ED] = {

    ClosureCleaner.clean(updateF)

    val newVTable = vTable.leftOuterJoin(updates).mapPartitions({ iter =>
      iter.map { case (vid, ((vdata, pids), update)) =>
        if (update.isDefined) {
          val newVdata = updateF(Vertex(vid, vdata), update.get)
          (vid, (newVdata, pids))
        } else {
          (vid, (vdata, pids))
        }
      }
    }, preservesPartitioning = true).cache()

    newGraph(newVTable.partitions.length, eTable.partitions.length, null, null, newVTable, eTable)
  }

    
  // Empty implementations of new primitives/syntactic sugars.
  // TODO(semih): Merge everything here.
   def filterEdges(p: Edge[ED] => Boolean): Graph[VD, ED] = {
	   throw new RuntimeException("filterEdges is not implemented in the base GraphImpl.scala")
   }
   
  def filterEdgesBasedOnSourceDestAndValue(p: EdgeTriplet[VD, ED] => Boolean): Graph[VD, ED] = {
	  throw new RuntimeException("filterEdgesBasedOnSourceDestAndValue is not implemented in the base GraphImpl.scala")
  }
 
  def filterVertices(p: Vertex[VD] => Boolean): Graph[VD, ED] = {
	   throw new RuntimeException("filterVertices is not implemented in the base GraphImpl.scala")
  }

  def filterVerticesUsingLocalEdges(direction: EdgeDirection,
    p: ((Vid, (VD, Option[Seq[Edge[ED]]]))) => Boolean): Graph[VD, ED] = {
	  throw new RuntimeException("filterVerticesBasedOnNeighborValues is not implemented in the base GraphImpl.scala")
  }
  
  def aggregateNeighborValuesFixedNumberOfIterations[A](
    direction: EdgeDirection,
    startVF: Vertex[VD] => Boolean,
    msgF: Vertex[VD] => Option[A],
    propagateAlongEdgeF: (A, ED) => A,
    updateF: ((Vertex[VD], Seq[Edge[ED]]), Option[Seq[A]]) => VD,
    numIter: Int) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
    throw new RuntimeException("aggregateNeighborValuesFixedNumberOfIterations is not implemented in the base GraphImpl.scala")  
  }
    
  def updateVerticesBasedOnNeighbors[A](
	direction: EdgeDirection,
    startVF: Vertex[VD] => Boolean,
    msgF: Vertex[VD] => Option[A],
    updateF: (Vertex[VD], Option[Seq[A]]) => VD) (implicit m: Manifest[VD], n:Manifest[A]): Graph[VD, ED] = {
    throw new RuntimeException("updateVerticesBasedOnNeighbors is not implemented in the base GraphImpl.scala")  
  }
  
  def updateVerticesUsingLocalEdges[VD2: ClassManifest](
    direction: EdgeDirection,
    map: (Vertex[VD], Option[Seq[Edge[ED]]]) => VD2): Graph[VD2, ED] = {
    throw new RuntimeException("updateVerticesBasedOnEdgeValues is not implemented in the base GraphImpl.scala")
  }

  def updateAnotherVertexBasedOnSelf[A](
    fromVertexF: Vertex[VD] => Boolean,
    idF: Vertex[VD] => Vid,
    msgF: Vertex[VD] => A,
    setF: (VD, Seq[A]) => VD) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
      throw new RuntimeException("updateAnotherVertexBasedOnSelf is not implemented in the base GraphImpl.scala")
  }

  def updateSelfUsingAnotherVertexsValue[A](
    verticesToUpdateP: Vertex[VD] => Boolean,
    idF: Vertex[VD] => Vid,
    msgF: Vertex[VD] => A,
    setFunction: (Vertex[VD], A) => VD) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
    throw new RuntimeException("updateSelfUsingAnotherVertexsValue is not implemented in the base GraphImpl.scala")
  }
  
  def pickRandomVertices(p: Vertex[VD] => Boolean, numVerticesToPick: Int): Seq[Vid] = {
     throw new RuntimeException("pickRandomVertices is not implemented in the base GraphImpl.scala")
  }

  def propagateAndAggregateFixedNumberOfIterations[A](
    direction: EdgeDirection,
    startVF: Vertex[VD] => Boolean,
    propagatedFieldF: VD => A,
    propagateAlongEdgeF: (A, ED) => A,
    setF: (Vertex[VD], Seq[A]) => VD,
    numIter: Int) (implicit m: Manifest[VD], n:Manifest[A]): Graph[VD, ED] = {
    throw new RuntimeException("propagateFromSomeUsingEdgeValueToAllInOutOrBothNeighbors is not implemented in the base GraphImpl.scala")
  }

  def simpleAggregateNeighborsFixedNumberOfIterations[A](
    aggregatedValueF: VD => A,
    aggrF: (A, Seq[A]) => A,
    setF: (VD, A) => VD,
    numIter: Int)(implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
    throw new RuntimeException("simpleAggregateNeighborsFixedNumberOfIterations is not implemented in the base GraphImpl.scala")
  }
   
  def simpleAggregateNeighborsFixedNumberOfIterationsReflection[A](
    aggregatedField: String,
    aggrF: (A, Seq[A]) => A,
    numIter: Int) (implicit m: Manifest[VD], n:Manifest[A]): Graph[VD, ED] = {
    throw new RuntimeException("simpleAggregateNeighborsFixedNumberOfIterationsReflection is not implemented in the base GraphImpl.scala")
  }

  def propagateAndAggregateUntilConvergence[A](
    direction: EdgeDirection,
    startVF: Vertex[VD] => Boolean,
    propagatedFieldF: VD => A,
    propagateAlongEdgeF: (A, ED) => A,
    setF: (Vertex[VD], Seq[A]) => VD) (implicit m: Manifest[VD], n: Manifest[A]) = {
    throw new RuntimeException("propagateUntilConvergence is not implemented in the base GraphImpl.scala")
  }: Graph[VD, ED]

  def formSuperVertices(groupByKeyF: VD => Vid, edgeAggrF: Seq[ED] => ED, vertexAggrF: Seq[VD] => VD,
    removeSelfLoops: Boolean) (implicit m: Manifest[VD]): Graph[VD, ED] = {
    throw new RuntimeException("formSuperVertices is not implemented in the base GraphImpl.scala")    
  }

  def aggregateGlobalValueOverVertices[A](direction: EdgeDirection,
    mapF: ((Vid, (VD, Option[Seq[spark.graph.Edge[ED]]]))) => Iterable[A],
    reduceF: (A, A) => A) (implicit m: Manifest[VD], n: Manifest[A]): A = {
    throw new RuntimeException("aggregateGlobalValueOverVertices is not implemented in the base GraphImpl.scala")     
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // Internals hidden from callers
  //////////////////////////////////////////////////////////////////////////////////////////////////

  // TODO: Support non-hash partitioning schemes.
  protected val vertexPartitioner = new HashPartitioner(numVertexPartitions)
  protected val edgePartitioner = new HashPartitioner(numEdgePartitions)

  /** Create a new graph but keep the current partitioning scheme. */
  protected def newGraph[VD2: ClassManifest, ED2: ClassManifest](
    vertices: RDD[Vertex[VD2]], edges: RDD[Edge[ED2]]): Graph[VD2, ED2] = {
    (new GraphImpl[VD2, ED2](vertices, edges)).withPartitioner(numVertexPartitions, numEdgePartitions)
  }

  protected lazy val eTable: RDD[(Pid, EdgePartition[ED])] = {
    if (_rawETable == null) {
      createETable(_rawEdges, numEdgePartitions)
    } else {
      _rawETable
    }
  }

  protected lazy val vTable: RDD[(Vid, (VD, Array[Pid]))] = {
    if (_rawVTable == null) {
      createVTable(_rawVertices, eTable, numVertexPartitions)
    } else {
      _rawVTable
    }
  }

  protected lazy val vTableReplicated: RDD[(Vid, VD)] = {
    // Join vid2pid and vTable, generate a shuffle dependency on the joined result, and get
    // the shuffle id so we can use it on the slave.
    vTable
      .flatMap { case (vid, (vdata, pids)) => pids.iterator.map { pid => (pid, (vid, vdata)) } }
      .partitionBy(edgePartitioner)
      .mapPartitions(
        { part => part.map { case(pid, (vid, vdata)) => (vid, vdata) } },
        preservesPartitioning = true)
  }
}


object GraphImpl {

  /**
   * Create the edge table RDD, which is much more efficient for Java heap storage than the
   * normal edges data structure (RDD[(Vid, Vid, ED)]).
   *
   * The edge table contains multiple partitions, and each partition contains only one RDD
   * key-value pair: the key is the partition id, and the value is an EdgePartition object
   * containing all the edges in a partition.
   */
  protected def createETable[ED: ClassManifest](edges: RDD[Edge[ED]], numPartitions: Int)
    : RDD[(Pid, EdgePartition[ED])] = {
    edges
      .map { e =>
        // Random partitioning based on the source vertex id.
        (math.abs(e.src) % numPartitions, (e.src, e.dst, e.data))
      }
      .partitionBy(new HashPartitioner(numPartitions))
      .mapPartitionsWithIndex({ (pid, iter) =>
        val edgePartition = new EdgePartition[ED]
        iter.foreach { case (_, (src, dst, data)) => edgePartition.add(src, dst, data) }
        edgePartition.trim()
        Iterator((pid, edgePartition))
      }, preservesPartitioning = true)
  }

  protected def createVTable[VD: ClassManifest, ED: ClassManifest](
      vertices: RDD[Vertex[VD]],
      eTable: RDD[(Pid, EdgePartition[ED])],
      numPartitions: Int)
    : RDD[(Vid, (VD, Array[Pid]))] = {
    val partitioner = new HashPartitioner(numPartitions)

    // A key-value RDD. The key is a vertex id, and the value is a list of
    // partitions that contains edges referencing the vertex.
    val vid2pid : RDD[(Int, Seq[Pid])] = eTable.mapPartitions { iter =>
      val (pid, edgePartition) = iter.next()
      val vSet = new it.unimi.dsi.fastutil.ints.IntOpenHashSet
      var i = 0
      while (i < edgePartition.srcIds.size) {
        vSet.add(edgePartition.srcIds.getInt(i))
        vSet.add(edgePartition.dstIds.getInt(i))
        i += 1
      }
      vSet.iterator.map { vid => (vid.intValue, pid) }
    }.groupByKey(partitioner)

    val foo = vertices.map { v => (v.id, v.data) }
    foo.join(foo)
 
    vertices
      .map { v => (v.id, v.data) }
      .partitionBy(partitioner)
      .leftOuterJoin(vid2pid)
      .mapValues {
        case (vdata, None)       => (vdata, Array.empty[Pid])
        case (vdata, Some(pids)) => (vdata, pids.toArray)
      }
  }
}

