
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import scala.math.BigInt

case class DBLPEdge(srcId: String, dstId: String, attr: Long)

case class DBLPEntry(title: String, authors: String, year: BigInt) {
  def isValid = {
    title != null && title.nonEmpty && authors != null && authors.nonEmpty && authors != "[]" && year != null
  }
}

class GraphX

object GraphX {
  private val datafile = "../../Downloads/test.json"



  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.
      master("local[4]").
      appName("Bruxa").
      config("spark.app.id", "Bruxa").
      getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext
    val dblpDF = spark.read.json(datafile).as[DBLPEntry]

    val edgeArray = dblpDF.filter {
      _.isValid
    }.map {
      row => {
        val gEdges = new ListBuffer[DBLPEdge]()

        val edges = row.authors.
          replace("[", "").
          replace("]", "").
          split(", ")

        if (edges.length > 1) {

          val combinations = edges combinations (2) map {
            case Array(a, b) => (a, b)
          } toList

          combinations foreach {
            c => {
              gEdges += DBLPEdge(c._1, c._2, edges.length)
            }
          }
        }

        gEdges.toList
      }
    }.filter {
      _.length > 0
    }.flatMap(x => x).map(x => Edge(x.srcId.hashCode, x.dstId.hashCode, x.attr)).collect()

    val vertexArray = dblpDF.filter { _.isValid } .map(row => {
      row.authors.
        replace("[", "").
        replace("]", "").
        split(", ")
    }).flatMap(x => x).map(x  => (x.hashCode.toLong, x)).collect()


    val edgesRDD = sc.parallelize(edgeArray)
    val vertexRDD = sc.parallelize(vertexArray)

    val graph = Graph(vertexRDD, edgesRDD)
    val mostCoauthors = graph.degrees.reduce((a, b) => if (a._2 > b._2) a else b)._1

    println("Edge count = " + graph.numEdges)
    println("Vertex count = " + graph.numVertices)
    println("Most co-authors: " + graph.vertices.filter(x => x._1 == mostCoauthors).first()._2)

    val numNeighbors = graph.collectNeighborIds(EdgeDirection.Either).map(x => (x._1, x._2.size))

    val sumUpRDD = graph.aggregateMessages[(Long, Long)](t => {
      t.sendToDst((t.dstId, t.attr))
    }, (a, b) => (a._1, a._2 + b._2))

    val smallAuthor = sumUpRDD.join(numNeighbors).map(x => (x._2._1._2.toDouble / x._2._2.toDouble, x._1)).sortByKey().first()._2
    val sa = graph.vertices.filter(x => x._1 == smallAuthor).first()._2

    println("Author with smallest average edge length: " + sa)


    // remove
//    val vertices = graph.vertices.map(_._1).collect()
//
//    val res = ShortestPaths.run(graph, vertices)
//
//    res.edges.toDF().show(false)
  }
}