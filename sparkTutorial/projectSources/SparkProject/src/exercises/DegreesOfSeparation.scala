package exercises

import org.apache.spark.{Accumulator, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.util.AccumulatorV2

object Color extends Enumeration {
  type Color = Value
  val WHITE, GRAY, BLACK = Value
}

object DegreesOfSeparation {

  // character to find
  val startCharId = 5306 // SpiderMan
  val targetCharId = 14 // ADAM

  var hitCounter:Option[Accumulator[Int]] = None

  // custom data types
  type BFSData = (Array[Int], Int, Color.Color)
  type BFSNode = (Int, BFSData)

  /* convertion line from input file to bfs node */
  def convertToBFS(line:String):BFSNode = {
    val fields = line.split("\\s+")
    val id = fields(0).toInt

    var connections:ArrayBuffer[Int] = ArrayBuffer()
    for (connection <- 1 to (fields.length-1)) {
      connections += fields(connection).toInt
    }

    var color:Color.Color = Color.WHITE
    var distance:Int = Int.MaxValue

    if (id == startCharId) {
      color = Color.GRAY
      distance = 0
    }

    (id, (connections.toArray, distance, color))
  }

  def createStartingRDD(sc:SparkContext): RDD[BFSNode] = {
    sc.textFile("Marvel-graph.txt").map(convertToBFS)
  }

  // map function
  // expands node into itself and its children
  def BFSMap(node:BFSNode):Array[BFSNode] = {

    val characterId = node._1
    val data = node._2

    val connections:Array[Int] = data._1
    val distance:Int = data._2
    var color:Color.Color = data._3

    var result:ArrayBuffer[BFSNode] = ArrayBuffer()

    if (color == Color.GRAY) {
      for (conn <- connections) {
        val newCharID = conn
        val newDist = distance + 1
        val newColor = Color.GRAY

        // have we stumbled accross searched character?
        if (targetCharId == conn) {
          if (hitCounter.isDefined) hitCounter.get.add(1)
        }

        val newEntry:BFSNode = (newCharID, (Array(), newDist, newColor))
        result += newEntry
      }

      // all nodes processed here...
      color = Color.BLACK
    }

    val thisEntry:BFSNode = (characterId, (connections, distance, color))
    result += thisEntry
    result.toArray
  }

  def BFSReduce(data1:BFSData, data2:BFSData):BFSData = {

    // extracting data we are combining
    val edges1:Array[Int] = data1._1
    val edges2:Array[Int] = data2._1
    val dist1:Int = data1._2
    val dist2:Int = data2._2
    val color1:Color.Color = data1._3
    val color2:Color.Color = data2._3

    // default node values
    var dist:Int = Int.MaxValue
    var color:Color.Color = Color.WHITE
    var edges:ArrayBuffer[Int] = ArrayBuffer()

    // TODO --- TYPE YOUR CODE HERE
    // merge edges

    // preserve minimum distance

    // preserve darkest color

    // TODO ^^^ TYPE YOUR CODE HERE

    // return result of reduction
    (edges.toArray, dist, color)
  }

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "DegreesOfSeparationContext")
    hitCounter = Some(sc.accumulator(0))

    var iterationRDD = createStartingRDD(sc)

    var iteration:Int = 0

    for (iteration <- 1 to 10) {
      println("BFS Iteration " + iteration)

      val mapped = iterationRDD.flatMap(BFSMap)
      println("Processing " + mapped.count() + " values.")

      if (hitCounter.isDefined) {
        val hitCount = hitCounter.get.value
        if (hitCount > 0) {
          println("Hit the target counter! From " + hitCount + " different directions")
        }
        return
      }

      // reducer work
      iterationRDD = mapped.reduceByKey(BFSReduce)
    }

    // TODO --- print results
  }
}
