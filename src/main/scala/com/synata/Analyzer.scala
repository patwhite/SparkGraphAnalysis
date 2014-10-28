package com.synata

import com.mongodb.BasicDBList
import org.apache.hadoop.conf.Configuration
import org.apache.spark.graphx.{EdgeDirection, Graph, Edge}
import org.apache.spark.{SparkContext, SparkConf}
import org.bson.BSONObject
import org.bson.types.BasicBSONList
import collection.JavaConversions._
import org.apache.spark.SparkContext._
import java.io._

/**
 * Created by patwhite on 10/27/14.
 */

case class Email(id: String, subject: String, from: List[String], to: List[String], cc: List[String], bcc: List[String])

case class CountTracker(inFromSource: Int, outToSource: Int) {
  def getRatio = if(outToSource == 0) Double.NegativeInfinity else inFromSource.toDouble / outToSource.toDouble
  def total = inFromSource + outToSource
  def incIn: CountTracker = copy(inFromSource + 1, outToSource)
  def incOut: CountTracker = copy(inFromSource, outToSource + 1)
  def merge(toMerge: CountTracker): CountTracker = {
    val out = CountTracker(toMerge.inFromSource + inFromSource, toMerge.outToSource + outToSource)
    out
  }
}

class Analyzer {
  private def getContext = {
    val driverPort = 7777
    val driverHost = "localhost"
    val master = "local"
    val app = "Spark Graph Analysis"

    val conf = new SparkConf() // skip loading external settings
      .set("spark.logConf", "true")
      .set("spark.driver.host", s"$driverHost")
      .set("spark.driver.port", s"$driverPort")
      .set("spark.cores.max", "2")
      .set("spark.executor.memory", "4g")
      .set("spark.eventLog.enabled", "false")

    new SparkContext(master, app, conf)
  }

  def exec() = {
    val context = getContext

    val jarFile = "target/scala-2.10/SparkGraphAnalysis-assembly-1.0.jar"
    val mongoDriverFile = "lib/mongo-java-driver-2.12.4.jar"
    val mongoHadoopFile = "lib/mongo-hadoop-core-1.3.0.jar"

    context.addJar(jarFile)
    context.addJar(mongoDriverFile)
    context.addJar(mongoHadoopFile)


    val mongoUri = s"mongodb://localhost"
    val hadoopConfig = new Configuration()

    hadoopConfig.set("mongo.input.uri", s"$mongoUri/graph.data")
    hadoopConfig.set("mongo.output.uri", s"$mongoUri/analysis.output")


    val rdd = context.newAPIHadoopRDD(hadoopConfig, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object], classOf[BSONObject])

    val mapped = rdd.map(m => Analyzer.parseObj(m._2))

    val emailListRDD = mapped.flatMap(m => m.from ::: m.to ::: m.cc ::: m.bcc)
                             .distinct()
                             .zipWithIndex()
                             .map(m => (m._2, m._1))

    val emailLookup = emailListRDD.collect().toMap
    val idLookup = emailLookup.map(m => (m._2, m._1)).toMap

    val edges = mapped.flatMap(m => Analyzer.parseEdge(m, idLookup))

    val g = Graph(emailListRDD, edges)

    Analyzer.outputGraph("MainGraph.gml", g)

    //Page Rank
    val pageRanked = g.pageRank(0.01).vertices

    val ranked = pageRanked.join(emailListRDD)
                           .sortBy(m => m._2._1, ascending = false)

    val prOutput = ranked.take(100) //Only grab the top 100
    prOutput.foreach { m =>
      println(s"${m._2._2}: ${m._2._1}")
    }

    val me = (prOutput.head._1, prOutput.head._2._2)
    println(s"I am: $me")


    //Strongly Connected
    val stronglyConnected = g.filter(m => m, vpred = (id, m: String) => id != me._1).stronglyConnectedComponents(10)


    val strongGraph = stronglyConnected.collectEdges(EdgeDirection.Out)

    strongGraph.collect().foreach { e =>
      val email = emailLookup(e._1)
      println(s"$email is strongly connected to:")
      val connected = e._2.map { inner =>
        if(inner.srcId == e._1) {
          emailLookup(inner.dstId)
        } else {
          emailLookup(inner.srcId)
        }
      }
      connected.distinct.foreach(c => println("\t" + c))
    }


    //Pregel
    val initialGraph = g.mapVertices((id, _) => CountTracker(0,0))
    val crs = initialGraph.pregel(CountTracker(0,0), activeDirection = EdgeDirection.Both, maxIterations = 1)(
      (src, currentCount, message) => {
        currentCount.merge(message)
      }, // Vertex Program
      triplet => {  // Send Message
        if(triplet.srcId == me._1) {
          //Send to dest
          Iterator((triplet.dstId, CountTracker(1, 0)))
        } else if(triplet.dstId == me._1) {
          //Send to src
          Iterator((triplet.srcId, CountTracker(0, 1)))
        } else {
          Iterator.empty
        }
      },
      (a,b) => {
        a.merge(b)
      } // Merge Message
    )

    val orderedCRS = crs.vertices.join(emailListRDD)
                .map(m => {
                  val ratio = m._2._1.getRatio
                  val total = m._2._1.total
                  val mult = if(ratio.isInfinite) Double.NegativeInfinity else ratio * total
                  (m._2._2, ratio, math.abs(1 - ratio), mult)
                })


//    println(orderedCRS.sortBy(m => m._3).collect.mkString("\n"))
//
//    println("------------")

    println(orderedCRS.sortBy(m => m._4, ascending = false).collect.mkString("\n"))

    context.stop()
  }
}

object Analyzer {
  def parseEdge(obj: Email, lookup: Map[String, Long]): List[Edge[String]] = {
    obj.from.flatMap { f =>
      val fId = lookup(f)
      val to = obj.to.map(t => Edge(fId, lookup(t), "SentEmailTo"))
      val cc = obj.cc.map(t => Edge(fId, lookup(t), "CCed"))
      val bcc = obj.bcc.map(t => Edge(fId, lookup(t), "BCCed"))
      to ::: cc ::: bcc
    }
  }

  def parseObj(obj: BSONObject): Email = {
    val from = if(obj.containsField("From")) obj.get("From").asInstanceOf[BasicDBList].map(_.toString).toList else List()
    val to = if(obj.containsField("To")) obj.get("To").asInstanceOf[BasicDBList].map(_.toString).toList else List()
    val cc = if(obj.containsField("CC")) obj.get("CC").asInstanceOf[BasicDBList].map(_.toString).toList else List()
    val bcc = if(obj.containsField("BCC")) obj.get("BCC").asInstanceOf[BasicDBList].map(_.toString).toList else List()

    val subject = obj.get("Subject").toString
    val id = obj.get("_id").toString
    Email(id, subject, from, to, cc, bcc)
  }

  def outputGraph(filename: String, graph: Graph[_, String]) = {
    val nodes = graph.vertices.collect()
    val edges = graph.edges.collect()

    val nodeOutputInt = nodes.map { node =>
      s"""node
        |[
        |id ${node._1}
        |label ${node._2}
        |]
      """.stripMargin
    }

    val nodeOutput = nodeOutputInt.mkString("\n")

    val edgeOutputInt = edges.map { edge =>
      s"""edge
         |[
         |source ${edge.srcId}
         |target ${edge.dstId}
         |label ${edge.attr}
         |]
       """.stripMargin
    }

    val edgeOutput = edgeOutputInt.mkString("\n")

    val output = s"""graph
        |[
        |$nodeOutput
        |$edgeOutput
        |]
      """.stripMargin

//    println(output)

    val writer = new PrintWriter(new File(filename))
    writer.write(output)
    writer.close()
  }
}
