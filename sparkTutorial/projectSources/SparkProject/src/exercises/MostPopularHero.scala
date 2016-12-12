package exercises

import org.apache.log4j._
import org.apache.spark.SparkContext

object MostPopularHero extends App {

  /* given line extracts heroID -> number of connections tuple*/
  def countCoOccurences(line:String) = {
    var elements = line.split("\\s+")
    (elements(0).toInt, elements.length-1)
  }

  /* given line extracts heroID -> heroName tuple */
  def parseNames(line:String):Option[(Int, String)] = {
    var fields = line.split("\"")
    if (fields.length > 1) {
      Some(fields(0).trim.toInt, fields(1))
    } else {
      None
    }
  }

  override def main(args: Array[String]): Unit = {

    // logger
    Logger.getLogger(MostPopularHero.getClass).setLevel(Level.DEBUG)

    // create SparkContext with use of every core on our local machine
    val sc = new SparkContext("local[*]", "MostPopularHeroContext")

    // heroID -> name RDD
    val namesRdd = sc.textFile("Marvel-names.txt").flatMap(parseNames)

    // heroID -> number of connections RDD
    val pairings = sc.textFile("Marvel-graph.txt").map(countCoOccurences)

    // TODO --- calculating result
    // make reduction of the same heroID RDDs

    // extracting result

    // TODO ^^^ calculating result
  }
}
