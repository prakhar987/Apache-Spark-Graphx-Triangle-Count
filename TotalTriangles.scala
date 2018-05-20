import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

val users: RDD[(VertexId, (String, String))] =
  sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
                       (5L, ("franklin", "prof")), (2L, ("istoica", "prof")), (8L, ("pastoistoica", "prof")) ))
// Create an RDD for edges
val relationships: RDD[Edge[String]] =
  sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
                       Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"), Edge(2L, 7L, "pi"), Edge(3L, 8L, "pi"), Edge(5L, 8L, "pi")))
// Define a default user in case there are relationship with missing user
val defaultUser = ("John Doe", "Missing")
// Build the initial Graph
//val graph = Graph(users, relationships, defaultUser)
val graph = GraphLoader.edgeListFile(sc, "data/graphx/3mb.txt", true).partitionBy(PartitionStrategy.RandomVertexCut)


val neighbours: VertexRDD[Array[VertexId]] = graph.ops.collectNeighborIds(EdgeDirection.Either)


val neighbourGraph = Graph(neighbours, graph.edges)
// neighbourGraph.vertices.collect.foreach(println(_))
val res = neighbourGraph.vertices.collect()

val olderFollowers = neighbourGraph.aggregateMessages[Int](
  triplet => { // Map Function
  	val iterSrc = triplet.srcAttr.iterator
    var counter: Int = 0
    while (iterSrc.hasNext) {
        val vidSrc = iterSrc.next()
        val iterDst = triplet.dstAttr.iterator
        while(iterDst.hasNext){
        	val vidDst = iterDst.next()
        	if(vidSrc == vidDst){
        		counter += 1
        	}
        }
        
      }
    triplet.sendToDst(counter)
    triplet.sendToSrc(counter)
  },
  // Add counter and age
  (a, b) => (a+b) // Reduce Function
)

val numTriangles: VertexRDD[Int] = olderFollowers.mapValues( (id, value) => value/2 )
val res = numTriangles.collect()

val finalData = sc.parallelize(numTriangles.collect())
var finalCount = (finalData.map(_._2).reduce((x,y) => x+y))/3

