
import java.io.FileWriter

import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, PartitionStrategy}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import scala.math.BigInt

case class DBLPEdge(srcId: String, dstId: String, attr: Long, attr_2: String)

case class DBLPEntry(title: String, authors: String, year: BigInt) {
  def isValid = {
    title != null && title.nonEmpty && authors != null && authors.nonEmpty && authors != "[]" && year != null
  }
}

class GraphX

object GraphX {
  private val datafile = "/Users/azazel/Downloads/dblp2.json"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.
      master("local[8]").
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
              gEdges += DBLPEdge(c._1, c._2, edges.length, row.title)
            }
          }
        }
        gEdges.toList
      }
    }.filter { _.nonEmpty }.flatMap(x => x).map(x => Edge(x.srcId.hashCode, x.dstId.hashCode, (x.attr, x.attr_2))).collect()

    val vertexArray = dblpDF.filter { _.isValid } .map(row => {
      row.authors.
        replace("[", "").
        replace("]", "").
        split(", ")
    }).flatMap(x => x).map(x  => (x.hashCode.toLong, x)).collect()

    val edgesRDD = sc.parallelize(edgeArray)
    val vertexRDD = sc.parallelize(vertexArray)

    val graph = Graph(vertexRDD, edgesRDD)

    val numEdges  = graph.numEdges
    val numVertices = graph.numVertices
    val mostCoauthorsID = graph.degrees.groupByKey().map(x => (x._2.sum, x._1)).sortByKey(false).first()._2
    val mostCoauthors = graph.vertices.filter(x => x._1 == mostCoauthorsID).first()._2

    // Find the author with the smallest average edge length.
    val numNeighbors = graph.collectNeighborIds(EdgeDirection.Either).map(x => (x._1, x._2.length))
    val sumUpRDD = graph.aggregateMessages[(Long, Long)](t => {
      t.sendToDst((t.dstId, t.attr._1))
    }, (a, b) => (a._1, a._2 + b._2))

    val smallAuthor = sumUpRDD.join(numNeighbors).map(x => (x._2._1._2.toDouble / x._2._2.toDouble, x._1)).sortByKey().first()._2
    val sa = graph.vertices.filter(x => x._1 == smallAuthor).first()._2

    // Choose a subgraph corresponding to the VLDB conference and compute the total number of triangles in this subgraph
    val subGraph = graph.subgraph(triplet => triplet.attr._2.contains("pvldb"))
    val triangleCount = subGraph.partitionBy(PartitionStrategy.RandomVertexCut)
                        .triangleCount().vertices
                        .map(x => x._2).sum() / 3

    // Choose a subgraph corresponding to the VLDB conference and compute PageRank of every node
    val pageRanks = subGraph.pageRank(0.0001).vertices.groupByKey().take(10).map(x => (x._1, x._2.sum))

    // For each author compute his "Erdös number" assuming that each edges have length 1 and assuming that they have length dependent on the number of authors
    // Find Erdös
    val erdosID = "Anna Esparcia-Alc\u00e1zar".hashCode //-1779292321 // Erdös id in dataset
//    val erdos = graph.vertices.filter(v => v._2.contains("Erd\u00f6s")).first()
//    println(erdos._1 + " " + erdos._2)

    // set Erdös number to 0 for Erdös, inifinity for the rest
    val newGraph = graph.mapVertices((id, name) => if (id == erdosID) 0.0 else Double.PositiveInfinity)

    //  calculate Erdös number for other authors
    val graphWithErdosNumbers = newGraph.pregel(Double.PositiveInfinity, 10, EdgeDirection.Either)(
      (_, num, newNum) => { if (num < newNum) num else newNum + 1 }, // Vertex Program
      triplet => { if (triplet.srcAttr < triplet.dstAttr) Iterator((triplet.dstId, triplet.srcAttr)) else Iterator.empty }, // Send message
      (a, b) => math.min(a, b) // Merge Message
    ).vertices.map(x => (x._2, x._1)).sortByKey().take(100)

    val fw = new FileWriter("output.txt", true)
    try {
      fw.write("number of edges = " + numEdges + "\n")
      fw.write("number of vertices = " + numVertices + "\n")
      fw.write("most coauthors = " + mostCoauthors + "\n")
      fw.write("Author with smallest average edge length: " + sa + "\n")
      fw.write("triangle count in vldb subgraph = " + triangleCount + "\n")
      fw.write("page ranks are = " + pageRanks.mkString("\n"))
      fw.write("smallest Erdös numbers are = " + graphWithErdosNumbers.mkString("\n"))
    }
    finally fw.close()
  }
}