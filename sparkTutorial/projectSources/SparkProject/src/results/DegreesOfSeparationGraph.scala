package results

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}

object DegreesOfSeparationGraph {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "MostPopularHeroGraphCtx")

    // vertices
    val names = sc.textFile("Marvel-names.txt")
    val vertices = names.flatMap(parseNames)

    // edges
    val lines = sc.textFile("Marvel-graph.txt")
    val edges = lines.flatMap(makeEdges)

    // graph
    val default = "Nobody"
    val graph = Graph(vertices, edges, default).cache()

    // degrees of sepeartion calculation

    // SpiderMan
    val root:VertexId = 5306
    val initialGraph = graph.mapVertices((id, _) => if (id == root) 0.0 else Double.PositiveInfinity)

    // pregel algorithm
    // pregel sends initial message of PositiveInfinity to every vertex and we set up 10 iterations
    val bfs = initialGraph.pregel(Double.PositiveInfinity, 10)(
      // program for vertex - it has to preserve the shortest distance between incoming message and current attribute
      (id, attr, msg) => math.min(attr, msg),

      // send message function - propagates out to all neighnours every iteration
      triplet => {
        if (triplet.srcAttr != Double.PositiveInfinity) {
          Iterator((triplet.dstId, triplet.srcAttr+1))
        } else {
          Iterator.empty
        }
      },

      // reduce operation - preserving minimum of messages received by vertex if it received more than one in each iteration
      (a, b) => math.min(a,b)
    )

    // get top 10 results
    bfs.vertices.join(vertices).take(10).foreach(println)
    // like in previous exercise SpiderMan to Adam
    println("\n\nDegrees from SpiderMan to ADAM")
    bfs.vertices.filter(x => x._1 == 14).collect.foreach(println)
  }

  def parseNames(line:String):Option[(VertexId, String)] = {
    var fields = line.split("\"")
    if (fields.length > 1) {
      val heroId:Long = fields(0).trim.toLong
      if (heroId < 6487) {
        return Some(fields(0).trim.toLong, fields(1))
      }
    }
    None
  }

  def makeEdges(line:String):List[Edge[Int]] = {
    import scala.collection.mutable.ListBuffer
    var edges = new ListBuffer[Edge[Int]]()
    val fields = line.split(" ")
    val origin = fields(0)

    for (x <- 1 to (fields.length-1)) {
      edges += Edge(origin.toLong, fields(x).toLong, 0)
    }

    edges.toList
  }
}
