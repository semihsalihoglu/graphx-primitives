package spark.graph

import spark.RDD

/**
 * The Graph abstractly represents a graph with arbitrary objects associated
 * with vertices and edges.  The graph provides basic operations to access and
 * manipulate the data associated with vertices and edges as well as the
 * underlying structure.  Like Spark RDDs, the graph is a functional
 * data-structure in which mutating operations return new graphs.
 *
 * @tparam VD The type of object associated with each vertex.
 *
 * @tparam ED The type of object associated with each edge
 */
abstract class Graph[VD: ClassManifest, ED: ClassManifest] {

  /**
   * Get the vertices and their data.
   *
   * @return An RDD containing the vertices in this graph
   *
   * @see Vertex for the vertex type.
   *
   * @todo should vertices return tuples instead of vertex objects?
   */
  def vertices: RDD[Vertex[VD]]

  /**
   * Get the Edges and their data as an RDD.  The entries in the RDD contain
   * just the source id and target id along with the edge data.
   *
   * @return An RDD containing the edges in this graph
   *
   * @see Edge for the edge type.
   * @see edgesWithVertices to get an RDD which contains all the edges along
   * with their vertex data.
   *
   * @todo Should edges return 3 tuples instead of Edge objects?  In this case
   * we could rename EdgeTriplet to Edge?
   */
  def edges: RDD[Edge[ED]]

  /**
   * Get the edges with the vertex data associated with the adjacent pair of
   * vertices.
   *
   * @return An RDD containing edge triplets.
   *
   * @example This operation might be used to evaluate a graph coloring where
   * we would like to check that both vertices are a different color.
   * {{{
   * type Color = Int
   * val graph: Graph[Color, Int] = Graph.textFile("hdfs://file.tsv")
   * val numInvalid = graph.edgesWithVertices()
   *   .map(e => if(e.src.data == e.dst.data) 1 else 0).sum
   * }}}
   *
   * @see edges() If only the edge data and adjacent vertex ids are required.
   *
   */
  def triplets: RDD[EdgeTriplet[VD, ED]]

  /**
   * Return a graph that is cached when first created. This is used to pin a
   * graph in memory enabling multiple queries to reuse the same construction
   * process.
   *
   * @see RDD.cache() for a more detailed explanation of caching.
   */
  def cache(): Graph[VD, ED]

  /**
   * Construct a new graph where each vertex value has been transformed by the
   * map function.
   *
   * @note This graph is not changed and that the new graph has the same
   * structure.  As a consequence the underlying index structures can be
   * reused.
   *
   * @param map the function from a vertex object to a new vertex value.
   *
   * @tparam VD2 the new vertex data type
   *
   * @example We might use this operation to change the vertex values from one
   * type to another to initialize an algorithm.
   * {{{
   * val rawGraph: Graph[(), ()] = Graph.textFile("hdfs://file")
   * val root = 42
   * var bfsGraph = rawGraph
   *   .mapVertices[Int](v => if(v.id == 0) 0 else Math.MaxValue)
   * }}}
   *
   */
  def mapVertices[VD2: ClassManifest](map: Vertex[VD] => VD2): Graph[VD2, ED]

  /**
   * Construct a new graph where each the value of each edge is transformed by
   * the map operation.  This function is not passed the vertex value for the
   * vertices adjacent to the edge.  If vertex values are desired use the
   * mapEdgesWithVertices function.
   *
   * @note This graph is not changed and that the new graph has the same
   * structure.  As a consequence the underlying index structures can be
   * reused.
   *
   * @param map the function from an edge object to a new edge value.
   *
   * @tparam ED2 the new edge data type
   *
   * @example This function might be used to initialize edge attributes.
   *
   */
  def mapEdges[ED2: ClassManifest](map: Edge[ED] => ED2): Graph[VD, ED2]

  /**
   * Construct a new graph where each the value of each edge is transformed by
   * the map operation.  This function passes vertex values for the adjacent
   * vertices to the map function.  If adjacent vertex values are not required,
   * consider using the mapEdges function instead.
   *
   * @note This graph is not changed and that the new graph has the same
   * structure.  As a consequence the underlying index structures can be
   * reused.
   *
   * @param map the function from an edge object to a new edge value.
   *
   * @tparam ED2 the new edge data type
   *
   * @example This function might be used to initialize edge attributes based
   * on the attributes associated with each vertex.
   * {{{
   * val rawGraph: Graph[Int, Int] = someLoadFunction()
   * val graph = rawGraph.mapEdgesWithVertices[Int]( edge =>
   *   edge.src.data - edge.dst.data)
   * }}}
   *
   */
  def mapTriplets[ED2: ClassManifest](
    map: EdgeTriplet[VD, ED] => ED2): Graph[VD, ED2]

  def correctEdges(): Graph[VD, ED]

  /**
   * Construct a new graph with all the edges reversed.  If this graph contains
   * an edge from a to b then the returned graph contains an edge from b to a.
   *
   */
  def reverse: Graph[VD, ED]

  /**
   * This function is used to compute a statistic for the neighborhood of each
   * vertex.
   *
   * This is one of the core functions in the Graph API in that enables
   * neighborhood level computation.  For example this function can be used to
   * count neighbors satisfying a predicate or implement PageRank.
   *
   * @note The returned RDD may contain fewer entries than their are vertices
   * in the graph.  This is because some vertices may not have neighbors or the
   * map function may return None for all neighbors.
   *
   * @param mapFunc the function applied to each edge adjacent to each vertex.
   * The mapFunc can optionally return None in which case it does not
   * contribute to the final sum.
   * @param mergeFunc the function used to merge the results of each map
   * operation.
   * @param direction the direction of edges to consider (e.g., In, Out, Both).
   * @tparam VD2 The returned type of the aggregation operation.
   *
   * @return A Spark.RDD containing tuples of vertex identifiers and thee
   * resulting value.  Note that the returned RDD may contain fewer vertices
   * than in the original graph since some vertices may not have neighbors or
   * the map function could return None for all neighbors.
   *
   * @example We can use this function to compute the average follower age for
   * each user
   * {{{
   * val graph: Graph[Int,Int] = loadGraph()
   * val averageFollowerAge: RDD[(Int, Int)] =
   *   graph.aggregateNeigbhros[(Int,Double)](
   *     (vid, edge) => (edge.otherVertex(vid).data, 1),
   *     (a, b) => (a._1 + b._1, a._2 + b._2),
   *     EdgeDirection.In)
   *     .mapValues{ case (sum,followers) => sum.toDouble / followers}
   * }}}
   *
   */
  def aggregateNeighbors[VD2: ClassManifest](
      mapFunc: (Vid, EdgeTriplet[VD, ED]) => Option[VD2],
      mergeFunc: (VD2, VD2) => VD2,
      direction: EdgeDirection)
    : RDD[(Vid, VD2)]


  /**
   * This function is used to compute a statistic for the neighborhood of each
   * vertex and returns a value for all vertices (including those without
   * neighbors).
   *
   * This is one of the core functions in the Graph API in that enables
   * neighborhood level computation. For example this function can be used to
   * count neighbors satisfying a predicate or implement PageRank.
   *
   * @note Because the a default value is provided all vertices will have a
   * corresponding entry in the returned RDD.
   *
   * @param mapFunc the function applied to each edge adjacent to each vertex.
   * The mapFunc can optionally return None in which case it does not
   * contribute to the final sum.
   * @param reduceFunc the function used to merge the results of each map
   * operation.
   * @param default the default value to use for each vertex if it has no
   * neighbors or the map function repeatedly evaluates to none
   * @param direction the direction of edges to consider (e.g., In, Out, Both).
   * @tparam VD2 The returned type of the aggregation operation.
   *
   * @return A Spark.RDD containing tuples of vertex identifiers and
   * their resulting value.  There will be exactly one entry for ever vertex in
   * the original graph.
   *
   * @example We can use this function to compute the average follower age
   * for each user
   * {{{
   * val graph: Graph[Int,Int] = loadGraph()
   * val averageFollowerAge: RDD[(Int, Int)] =
   *   graph.aggregateNeigbhros[(Int,Double)](
   *     (vid, edge) => (edge.otherVertex(vid).data, 1),
   *     (a, b) => (a._1 + b._1, a._2 + b._2),
   *     -1,
   *     EdgeDirection.In)
   *     .mapValues{ case (sum,followers) => sum.toDouble / followers}
   * }}}
   *
   * @todo Should this return a graph with the new vertex values?
   *
   */
  def aggregateNeighbors[VD2: ClassManifest](
      mapFunc: (Vid, EdgeTriplet[VD, ED]) => Option[VD2],
      reduceFunc: (VD2, VD2) => VD2,
      default: VD2, // Should this be a function or a value?
      direction: EdgeDirection)
    : RDD[(Vid, VD2)]


  /**
   * Join the vertices with an RDD and then apply a function from the the
   * vertex and RDD entry to a new vertex value and type.  The input table should
   * contain at most one entry for each vertex.  If no entry is provided the
   * map function is invoked passing none.
   *
   * @tparam U the type of entry in the table of updates
   * @tparam VD2 the new vertex value type
   *
   * @param table the table to join with the vertices in the graph.  The table
   * should contain at most one entry for each vertex.
   * @param mapFunc the function used to compute the new vertex values.  The
   * map function is invoked for all vertices, even those that do not have a
   * corresponding entry in the table.
   *
   * @example This function is used to update the vertices with new values
   * based on external data.  For example we could add the out degree to each
   * vertex record
   * {{{
   * val rawGraph: Graph[(),()] = Graph.textFile("webgraph")
   * val outDeg: RDD[(Int, Int)] = rawGraph.outDegrees()
   * val graph = rawGraph.leftJoinVertices[Int,Int](outDeg,
   *   (v, deg) => deg.getOrElse(0) )
   * }}}
   *
   * @todo Should this function be curried to enable type inference?  For
   * example
   * {{{
   * graph.leftJoinVertices(tbl)( (v, row) => row.getOrElse(0) )
   * }}}
   * @todo Is leftJoinVertices the right name?
   */
  def leftJoinVertices[U: ClassManifest, VD2: ClassManifest](
      table: RDD[(Vid, U)],
      mapFunc: (Vertex[VD], Option[U]) => VD2)
    : Graph[VD2, ED]

  /**
   * Join the vertices with an RDD and then apply a function from the the
   * vertex and RDD entry to a new vertex value.  The input table should
   * contain at most one entry for each vertex.  If no entry is provided the
   * map function is skipped and the old value is used.
   *
   * @tparam U the type of entry in the table of updates
   * @param table the table to join with the vertices in the graph.  The table
   * should contain at most one entry for each vertex.
   * @param mapFunc the function used to compute the new vertex values.  The
   * map function is invoked only for vertices with a corresponding entry in
   * the table otherwise the old vertex value is used.
   *
   * @note for small tables this function can be much more efficient than
   * leftJoinVertices
   *
   * @example This function is used to update the vertices with new values
   * based on external data.  For example we could add the out degree to each
   * vertex record
   * {{{
   * val rawGraph: Graph[Int,()] = Graph.textFile("webgraph")
   *   .mapVertices(v => 0)
   * val outDeg: RDD[(Int, Int)] = rawGraph.outDegrees()
   * val graph = rawGraph.leftJoinVertices[Int,Int](outDeg,
   *   (v, deg) => deg )
   * }}}
   *
   * @todo Should this function be curried to enable type inference?  For
   * example
   * {{{
   * graph.joinVertices(tbl)( (v, row) => row )
   * }}}
   */
  def joinVertices[U: ClassManifest](
      table: RDD[(Vid, U)],
      mapFunc: (Vertex[VD], U) => VD)
    : Graph[VD, ED]
 
  // Save a copy of the GraphOps object so there is always one unique GraphOps object
  // for a given Graph object, and thus the lazy vals in GraphOps would work as intended.
  val ops = new GraphOps(this)
 
  // New Primitives/Syntactic sugars (by semih)  
 /**
   * Returns a new graph with only the edges that satisfy the predicate p applied to the edge's:
   * (1) src vertex ID, (2) dst vertex ID, (3) data
   * 
   * @param p predicate applied to Edge[ED]
   */
  def filterEdges(p: Edge[ED] => Boolean): Graph[VD, ED]

  /**
   * Returns a new graph with only the edges that satisfy the predicate p applied to the edge's:
   * (1) src vertex and its data, (2) dst vertex and its data, (3) data (all three contained
   * in an EdgeTriplet)
   * 
   * @param p predicate applied to EdgeTriplet[VD, ED]
   */
  def filterEdgesBasedOnSourceDestAndValue(p: EdgeTriplet[VD, ED] => Boolean): Graph[VD, ED]

  /** 
   *  Returns a new graph with only the vertices that satisfy the predicate p, where p is a predicate applied
   *  only to vertex ID and its data.
   *  All edges that have their source or destination vertices as one of the removed edges are also cleaned.
   *  
   *  @param direction direction of the edges the predicate wants to look at
   *  @param p predicate to apply to (Vid, Vertex[VD])
   **/
  def filterVertices(p: Vertex[VD] => Boolean): Graph[VD, ED]
   
  /** 
   *  Returns a new graph with only the vertices that satisfy the predicate p, where p is a predicate applied
   *  to vertex ID and its data and all its specified edges (in, out or both).
   *  All edges that have their source or destination vertices as one of the removed edges are also cleaned.
   *  
   *  @param direction direction of the edges the predicate wants to look at
   *  @param p predicate to apply to (Vid, (VD, Seq[Edge[ED]]))
   **/
  def filterVerticesBasedOnEdgeValues(direction: EdgeDirection,
    p: ((Vid, (VD, Option[Seq[Edge[ED]]]))) => Boolean): Graph[VD, ED]

  def filterVerticesBasedOnOutgoingEdgeValues(p: ((Vid, (VD, Option[Seq[Edge[ED]]]))) => Boolean): Graph[VD, ED] = {
    filterVerticesBasedOnEdgeValues(EdgeDirection.Out, p)
  }

  def filterVerticesBasedOnIncomingEdgeValues(p: ((Vid, (VD, Option[Seq[Edge[ED]]]))) => Boolean): Graph[VD, ED] = {
    filterVerticesBasedOnEdgeValues(EdgeDirection.In, p)
  }

  def filterVerticesBasedOnBothInAndOutEdgeValues(p: ((Vid, (VD, Option[Seq[Edge[ED]]]))) => Boolean): Graph[VD, ED] = {
    filterVerticesBasedOnEdgeValues(EdgeDirection.Both, p)
  }

  def updateVerticesBasedOnEdgeValues[VD2: ClassManifest](
    direction: EdgeDirection,
    map: (Vertex[VD], Option[Seq[Edge[ED]]]) => VD2): Graph[VD2, ED]

  def updateVerticesBasedOnOutgoingEdgeValues[VD2: ClassManifest](
    map: (Vertex[VD], Option[Seq[Edge[ED]]]) => VD2): Graph[VD2, ED] = {
    updateVerticesBasedOnEdgeValues(EdgeDirection.Out, map)
  }

  def updateVerticesBasedOnIncomingEdgeValues[VD2: ClassManifest](
    map: (Vertex[VD], Option[Seq[Edge[ED]]]) => VD2): Graph[VD2, ED] = {
    updateVerticesBasedOnEdgeValues(EdgeDirection.In, map)
  }

  def updateVerticesBasedOnBothIncomingAndOutgoingEdgeValues[VD2: ClassManifest](
    map: (Vertex[VD], Option[Seq[Edge[ED]]]) => VD2): Graph[VD2, ED] = {
    updateVerticesBasedOnEdgeValues(EdgeDirection.Both, map)
  }

  def mapReduceOverVerticesUsingEdges[A](direction: EdgeDirection,
    mapF: ((Vid, (VD, Option[Seq[spark.graph.Edge[ED]]]))) => Iterable[A],
    reduceF: (A, A) => A) (implicit m: Manifest[VD], n: Manifest[A]): A
  /**
   * For each vertex x, sets x's value based on another vertex y's value, where the ID of y is stored
   * somewhere in x's value.
   * 
   * @param idFunction the function that takes vertex x and returns the ID of y
   * @param setFunction takes (y's value, x's value) and return x's new value
   */
  def updateVertexValueBasedOnAnotherVertexsValue(idFunction: Vertex[VD] => Vid,
    setFunction: (VD, Vertex[VD]) => VD): Graph[VD, ED]
  
  /**
   * For each vertex x, sets x's toField value based on another vertex y's fromField value,
   * where the ID of y is stored in x's idField. The field names are passed as strings and they are
   * accessed and set through reflection.
   * 
   * @param idField field in x's value that stores the id of y
   * @param fromField field in y's value to copy from
   * @param toField field in x's value to copy to
   */
  def updateVertexValueBasedOnAnotherVertexsValueReflection(idField: String,
    fromField: String, toField: String) (implicit m: Manifest[VD]): Graph[VD, ED]
  
  /**
   * Picks a random vertex from the graph and returns its identifier.
   */
  def pickRandomVertex(): Vid
  
  def propagateFixedNumberOfIterations[A](
    direction: EdgeDirection,
    startVF: Vertex[VD] => Boolean,
    propagatedFieldF: VD => A,
    propagateAlongEdgeF: (A, ED) => A,
    aggrF: (A, Seq[A]) => A,
    setF: (VD, A) => VD,
    numIter: Int) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED]
  
  def propagateFixedNumberOfIterationsReflection[A](
    direction: EdgeDirection,
    startVF: Vertex[VD] => Boolean,
    propagatedField: String,
    propagateAlongEdgeF: (A, ED) => A,
    aggrF: (A, Seq[A]) => A,
    numIter: Int) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
    propagateFixedNumberOfIterations(direction, startVF,
      vvals => m.erasure.asInstanceOf[Class[VD]].getMethods().find(_.getName == propagatedField).get.invoke(
        vvals).asInstanceOf[A],
      propagateAlongEdgeF, aggrF,
      (vvals, finalFieldValue: A) => {
        manifest[VD].erasure.asInstanceOf[Class[VD]].getMethods().find(_.getName == propagatedField + "_$eq").get.invoke(
          vvals, finalFieldValue.asInstanceOf[Object])
        vvals
      }, numIter)
  }

  def propagateForwardFixedNumberOfIterations[A](
    startVF: Vertex[VD] => Boolean,
    propagatedFieldF: VD => A,
    propagateAlongEdgeF: (A, ED) => A,
    aggrF: (A, Seq[A]) => A,
    setF: (VD, A) => VD,
    numIter: Int) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
    propagateFixedNumberOfIterations(
      EdgeDirection.Out, startVF, propagatedFieldF, propagateAlongEdgeF, aggrF, setF, numIter)
  }
  
  def propagateInTransposeFixedNumberOfIterations[A](
    startVF: Vertex[VD] => Boolean,
    propagatedFieldF: VD => A,
    propagateAlongEdgeF: (A, ED) => A,
    aggrF: (A, Seq[A]) => A,
    setF: (VD, A) => VD,
    numIter: Int) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
    propagateFixedNumberOfIterations(
      EdgeDirection.In, startVF, propagatedFieldF, propagateAlongEdgeF, aggrF, setF, numIter)
  }
  
  def propagateInBothDirectionsFixedNumberOfIterations[A](
    startVF: Vertex[VD] => Boolean,
    propagatedFieldF: VD => A,
    propagateAlongEdgeF: (A, ED) => A,
    aggrF: (A, Seq[A]) => A,
    setF: (VD, A) => VD,
    numIter: Int) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
    propagateFixedNumberOfIterations(
      EdgeDirection.Both, startVF, propagatedFieldF, propagateAlongEdgeF, aggrF, setF, numIter)
  }

  def propagateUntilConvergence[A](
    direction: EdgeDirection,
    startVF: Vertex[VD] => Boolean,
    propagatedFieldF: VD => A,
    propagateAlongEdgeF: (A, ED) => A,
    aggrF: (A, Seq[A]) => A,
    setF: (VD, A) => VD) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED]
  
  def propagateUntilConvergenceReflection[A](
    direction: EdgeDirection,
    startVF: Vertex[VD] => Boolean,
    propagatedField: String,
    propagateAlongEdgeF: (A, ED) => A,
    aggrF: (A, Seq[A]) => A) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
     propagateUntilConvergence(direction, startVF,
      vvals => m.erasure.asInstanceOf[Class[VD]].getMethods().find(_.getName == propagatedField).get.invoke(
        vvals).asInstanceOf[A],
      propagateAlongEdgeF, aggrF,
      (vvals, finalFieldValue: A) => {
        manifest[VD].erasure.asInstanceOf[Class[VD]].getMethods().find(_.getName == propagatedField + "_$eq").get.invoke(
          vvals, finalFieldValue.asInstanceOf[Object])
        vvals
      })
  }
  
  def propagateForwardUntilConvergence[A](
    startVF: Vertex[VD] => Boolean,
    propagatedFieldF: VD => A,
    propagateAlongEdgeF: (A, ED) => A,
    aggrF: (A, Seq[A]) => A,
    setF: (VD, A) => VD) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
    propagateUntilConvergence(
          EdgeDirection.Out,
          startVF,
          propagatedFieldF,
          propagateAlongEdgeF,
          aggrF,
          setF)
  }
  
  def propagateForwardUntilConvergenceReflection[A](
    startVF: Vertex[VD] => Boolean,
    propagatedField: String,
    propagateAlongEdgeF: (A, ED) => A,
    aggrF: (A, Seq[A]) => A) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
    propagateUntilConvergenceReflection(
          EdgeDirection.Out,
          startVF,
          propagatedField,
          propagateAlongEdgeF,
          aggrF)
  }

  def propagateForwardUntilConvergenceFromOneReflection[A](
    vertexID: Vid,
    propagatedField: String,
    propagateAlongEdgeF: (A, ED) => A,
    aggrF: (A, Seq[A]) => A)(implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
    propagateUntilConvergenceReflection(
      EdgeDirection.Out,
      (vertex: Vertex[VD]) => {
        val retVal = vertex.id == vertexID;
        println("isStarting from vertex: " + vertex.id + " " + retVal);
        retVal
      },
      propagatedField,
      propagateAlongEdgeF,
      aggrF)
  }

  def propagateInTransposeUntilConvergenceFromOneReflection[A](
    vertexID: Vid,
    propagatedField: String,
    propagateAlongEdgeF: (A, ED) => A,
    aggrF: (A, Seq[A]) => A)(implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
    propagateUntilConvergenceReflection(
      EdgeDirection.In,
      (vertex: Vertex[VD]) => {
        val retVal = vertex.id == vertexID;
        println("isStarting from vertex: " + vertex.id + " " + retVal);
        retVal
      },
      propagatedField,
      propagateAlongEdgeF,
      aggrF)
  }

  def propagateInBothDirectionsUntilConvergenceFromOneReflection[A](
    vertexID: Vid,
    propagatedField: String,
    propagateAlongEdgeF: (A, ED) => A,
    aggrF: (A, Seq[A]) => A)(implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
    propagateUntilConvergenceReflection(
      EdgeDirection.Both,
      (vertex: Vertex[VD]) => {
        val retVal = vertex.id == vertexID;
        println("isStarting from vertex: " + vertex.id + " " + retVal);
        retVal
      },
      propagatedField,
      propagateAlongEdgeF,
      aggrF)
  }
  def propagateInTransposeUntilConvergence[A](
    startVF: Vertex[VD] => Boolean,
    propagatedFieldF: VD => A,
    propagateAlongEdgeF: (A, ED) => A,
    aggrF: (A, Seq[A]) => A,
    setF: (VD, A) => VD) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
    propagateUntilConvergence(
          EdgeDirection.In,
          startVF,
          propagatedFieldF,
          propagateAlongEdgeF,
          aggrF,
          setF)
  }
  
  def propagateInTransposeUntilConvergenceReflection[A](
    startVF: Vertex[VD] => Boolean,
    propagatedField: String,
    propagateAlongEdgeF: (A, ED) => A,
    aggrF: (A, Seq[A]) => A,
    setF: (VD, A) => VD) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
    propagateUntilConvergenceReflection(
          EdgeDirection.In,
          startVF,
          propagatedField,
          propagateAlongEdgeF,
          aggrF)
  }

  def propagateInBothDirectionsUntilConvergence[A](
    startVF: Vertex[VD] => Boolean,
    propagatedFieldF: VD => A,
    propagateAlongEdgeF: (A, ED) => A,
    aggrF: (A, Seq[A]) => A,
    setF: (VD, A) => VD) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
    propagateUntilConvergence(
          EdgeDirection.Both,
          startVF,
          propagatedFieldF,
          propagateAlongEdgeF,
          aggrF,
          setF)
  }

  def propagateInBothDirectionsUntilConvergenceReflection[A](
    startVF: Vertex[VD] => Boolean,
    propagatedField: String,
    propagateAlongEdgeF: (A, ED) => A,
    aggrF: (A, Seq[A]) => A) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
    propagateUntilConvergenceReflection(
          EdgeDirection.Both,
          startVF,
          propagatedField,
          propagateAlongEdgeF,
          aggrF)
  }

  def simplePropagate[A](
   direction: EdgeDirection,
   startVF: Vertex[VD] => Boolean,
   propagatedFieldF: VD => A,
   aggrF: (A, Seq[A]) => A,
   setF: (VD, A) => VD) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
    propagateUntilConvergence(direction, startVF, propagatedFieldF,
      (vertexField: A, edgeValue: ED) => vertexField,
      aggrF,
      setF)
  }

  def simplePropagateReflection[A](
   direction: EdgeDirection,
   startVF: Vertex[VD] => Boolean,
   propagatedField: String,
   aggrF: (A, Seq[A]) => A) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
     propagateUntilConvergenceReflection(
       direction,
       startVF,
       propagatedField,
       (vertexField: A, edgeValue: ED) => vertexField,
       aggrF)
  }

  def simplePropagateFixedNumberOfIterationsReflection[A](
   direction: EdgeDirection,
   startVF: Vertex[VD] => Boolean,
   propagatedField: String,
   aggrF: (A, Seq[A]) => A,
   numIter: Int) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
     propagateFixedNumberOfIterationsReflection(
       direction,
       startVF,
       propagatedField,
       (vertexField: A, edgeValue: ED) => vertexField,
       aggrF,
       numIter)
  }

 def simplePropagateForwardReflection[A](
   startVF: Vertex[VD] => Boolean,
   propagatedField: String,
   aggrF: (A, Seq[A]) => A) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
	simplePropagateReflection(
	  EdgeDirection.Out,
	  startVF,
	  propagatedField,
	  aggrF)
  }

 def simplePropagateInTransposeReflection[A](
   startVF: Vertex[VD] => Boolean,
   propagatedField: String,
   aggrF: (A, Seq[A]) => A) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
	simplePropagateReflection(
	  EdgeDirection.In,
	  startVF,
	  propagatedField,
	  aggrF)
  }

 def simplePropagateInBothDirectionsReflection[A](
   startVF: Vertex[VD] => Boolean,
   propagatedField: String,
   aggrF: (A, Seq[A]) => A) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
	simplePropagateReflection(
	  EdgeDirection.Both,
	  startVF,
	  propagatedField,
	  aggrF)
  }

 def simplePropagateForwardFromOne[A](
   vertexID: Vid,
   propagatedFieldF: VD => A,
   aggrF: (A, Seq[A]) => A,
   setF: (VD, A) => VD) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
	simplePropagate(
	  EdgeDirection.Out,
      (vertex: Vertex[VD]) => { val retVal = vertex.id == vertexID; println("isStarting from vertex: " + vertex.id + " " + retVal); retVal},
	  propagatedFieldF,
	  aggrF,
	  setF)
  }

  def simplePropagateForwardFromOneReflection[A](
   vertexID: Vid,
   propagatedField: String,
   aggrF: (A, Seq[A]) => A) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
	simplePropagateReflection(
	  EdgeDirection.Out,
      (vertex: Vertex[VD]) => {
        val retVal = vertex.id == vertexID;
        println("isStarting from vertex: " + vertex.id + " " + retVal);
        retVal},
	  propagatedField,
	  aggrF)
  }
    
 def simplePropagateInTransposeFromOne[A](
   vertexID: Vid,
   propagatedFieldF: VD => A,
   aggrF: (A, Seq[A]) => A,
   setF: (VD, A) => VD) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
	simplePropagate(
	  EdgeDirection.In,
      (vertex: Vertex[VD]) => vertex.id == vertexID,
	  propagatedFieldF,
	  aggrF,
	  setF)
  }
  
  def simplePropagateInTransposeFromOneReflection[A](
   vertexID: Vid,
   propagatedField: String,
   aggrF: (A, Seq[A]) => A) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
	simplePropagateReflection(
	  EdgeDirection.In,
      (vertex: Vertex[VD]) => {
        val retVal = vertex.id == vertexID;
        println("isStarting from vertex: " + vertex.id + " " + retVal);
        retVal},
	  propagatedField,
	  aggrF)
 }

 def simplePropagateInBothDirectionsFromOne[A](
   vertexID: Vid,
   propagatedFieldF: VD => A,
   aggrF: (A, Seq[A]) => A,
   setF: (VD, A) => VD) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
	simplePropagate(
	  EdgeDirection.Both,
      (vertex: Vertex[VD]) => vertex.id == vertexID,
	  propagatedFieldF,
	  aggrF,
	  setF)
  }
  
  def simplePropagateInBothDirectionsFromOneReflection[A](
   vertexID: Vid,
   propagatedField: String,
   aggrF: (A, Seq[A]) => A) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
	simplePropagateReflection(
	  EdgeDirection.Both,
      (vertex: Vertex[VD]) => {
        val retVal = vertex.id == vertexID;
        println("isStarting from vertex: " + vertex.id + " " + retVal);
        retVal},
	  propagatedField,
	  aggrF)
 }

 def simplePropagateForwardFromAll[A](
   propagatedFieldF: VD => A,
   aggrF: (A, Seq[A]) => A,
   setF: (VD, A) => VD) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
	simplePropagate(
	  EdgeDirection.Out,
	  (vertex: Vertex[VD]) => true,
	  propagatedFieldF,
	  aggrF,
	  setF)
  }

 def simplePropagateForwardFromAllReflection[A](
   propagatedField: String,
   aggrF: (A, Seq[A]) => A) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
	simplePropagateReflection(
	  EdgeDirection.Out,
	  (vertex: Vertex[VD]) => true,
	  propagatedField,
	  aggrF)
  }
   
 def simpleAggregateNeighborsFixedNumberOfIterationsReflection[A](
    aggregatedField: String,
    aggrF: (A, Seq[A]) => A,
    numIter: Int) (implicit m: Manifest[VD], n:Manifest[A]): Graph[VD, ED]

 def simpleAggregateNeighborsFixedNumberOfIterations[A](
    aggregateValueF: VD => A,
    aggrF: (A, Seq[A]) => A,
    setF: (VD, A) => VD,
    numIter: Int) (implicit m: Manifest[VD], n:Manifest[A]): Graph[VD, ED]

 def simplePropagateFixedNumberOfIterationsForwardFromAllReflection[A](
   propagatedField: String,
   aggrF: (A, Seq[A]) => A,
   numIter: Int) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
	simplePropagateFixedNumberOfIterationsReflection(
	  EdgeDirection.Out,
	  (vertex: Vertex[VD]) => true,
	  propagatedField,
	  aggrF,
	  numIter)
  }

  def simplePropagateInTransposeFromAll[A](
   propagatedFieldF: VD => A,
   aggrF: (A, Seq[A]) => A,
   setF: (VD, A) => VD) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
	simplePropagate(
	  EdgeDirection.In,
	  (vertex: Vertex[VD]) => true,
	  propagatedFieldF,
	  aggrF,
	  setF)
  }

 def simplePropagateInTransposeFromAllReflection[A](
   propagatedField: String,
   aggrF: (A, Seq[A]) => A) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
	simplePropagateReflection(
	  EdgeDirection.In,
	  (vertex: Vertex[VD]) => true,
	  propagatedField,
	  aggrF)
  }

  def simplePropagateInBothDirectionsFromAll[A](
   propagatedFieldF: VD => A,
   aggrF: (A, Seq[A]) => A,
   setF: (VD, A) => VD) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
	simplePropagate(
	  EdgeDirection.Both,
	  (vertex: Vertex[VD]) => true,
	  propagatedFieldF,
	  aggrF,
	  setF)
  }

  def simplePropagateInBothDirectionsFromAllReflection[A](
   propagatedField: String,
   aggrF: (A, Seq[A]) => A) (implicit m: Manifest[VD], n: Manifest[A]): Graph[VD, ED] = {
	simplePropagateReflection(
	  EdgeDirection.Both,
	  (vertex: Vertex[VD]) => true,
	  propagatedField,
	  aggrF)
  }

  def formSuperVertices(groupByKeyF: VD => Vid, edgeAggrF: Seq[ED] => ED, vertexAggrF: Seq[VD] => VD,
    removeSelfLoops: Boolean)
  	(implicit m: Manifest[VD]): Graph[VD, ED]
}

object Graph {

  import spark.graph.impl._
  import spark.SparkContext._

  def apply(rawEdges: RDD[(Int, Int)], uniqueEdges: Boolean = true): Graph[Int, Int] = {
    // Reduce to unique edges.
    val edges: RDD[Edge[Int]] =
      if (uniqueEdges) {
        rawEdges.map((_, 1)).reduceByKey(_ + _).map { case ((s, t), cnt) => Edge(s, t, cnt) }
      } else {
        rawEdges.map { case (s, t) => Edge(s, t, 1) }
      }
    // Determine unique vertices
    val vertices: RDD[Vertex[Int]] = edges.flatMap{ case Edge(s, t, cnt) => Array((s, 1), (t, 1)) }
      .reduceByKey(_ + _)
      .map{ case (id, deg) => Vertex(id, deg) }
    // Return graph
    new GraphImplWithPrimitives(vertices, edges)
  }

  def apply[VD: ClassManifest, ED: ClassManifest](
      vertices: RDD[Vertex[VD]], edges: RDD[Edge[ED]]): Graph[VD, ED] = {
    new GraphImplWithPrimitives(vertices, edges)
  }

  implicit def graphToGraphOps[VD: ClassManifest, ED: ClassManifest](g: Graph[VD, ED]) = g.ops
}
