package exercises

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, PartitionStrategy, VertexId}

object PageRankOfHeroes {

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

    // calculating PageRank
    // TODO your code goes here

    // calcularing triangle count
    // TODO your code goes here
  }


}
