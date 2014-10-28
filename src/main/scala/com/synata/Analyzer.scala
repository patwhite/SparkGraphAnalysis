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


    context.stop()
  }
}

object Analyzer {
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
