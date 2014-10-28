package com.synata

import akka.actor.ActorSystem
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkContext, SparkConf}
import org.bson.BSONObject

object ApplicationMain extends App {
  if(args.length < 1) {
    println("Usage load <Access Token> or analyze")
  } else {
    args.head.toLowerCase match {
      case "analyze" =>
        val analyzer = new Analyzer
        analyzer.exec()

      case "load" =>
        if(args.length != 2)
          println("Usage load <Access Token> or analyze")

        val dl = new DataLoader(args(1))
        dl.load()
    }
  }
}