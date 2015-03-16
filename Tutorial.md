First:
Get the sample code!
https://github.com/Synata/SparkGraphAnalysis


You'll need to download spark from 	
http://d3kbcqa49mib13.cloudfront.net/spark-1.1.0-bin-hadoop2.4.tgz

Unzip it, and just take the spark_assembly jar from lib/

Or, if that link doesn't work, go to:
https://spark.apache.org/downloads.html
and select Pre-built for Hadoop 2.4.

--------
Make sure Mongo is running at localhost:27017

To start with, we need some data to graph analyze. The easiest way to get some relevant graph data is your email!

We'll be using OAuth / API calls to get the data. You'll need an access token.
The easiest way to get your Gmail Access Token is with the Gmail API Playground. Go to:
https://developers.google.com/gmail/api/v1/reference/users/messages/list
and under "Try It" click the "Authorize Oauth" button. Open the developer console, and find the request that ends with /me - Look for the header: 

```
"Authorization: Bearer <TOKEN>" 
```

Copy and paste <TOKEN> into the command:

The usage is:
```
sbt "run load <Gmail Access Token>"
```

This will dump your gmail and load metadata into Mongo.

---------

We're going to be working on the analyzer, so add this to exec:

```
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
```
  
```
val context = getContext
try {
	val rdd = sc.parallelize(List(1, 2, 3, 4))
	val arr = rdd.collect()
	println("Array: ")
	arr.foreach(m =>
		println(m)
	)
} finally { 
	context.stop()
}
```

Test that it’s all working!

sbt run


Replace SparkContext:
```
val jarFile = "target/scala-2.10/SparkGraphAnalysis-assembly-1.0.jar"
val mongoDriverFile = "lib/mongo-java-driver-2.12.4.jar"
val mongoHadoopFile = "lib/mongo-hadoop-core-1.3.0.jar"

context.addJar(jarFile)
context.addJar(mongoDriverFile)
context.addJar(mongoHadoopFile)
```

Test it out, make sure it’s finding the jars

```
sbt run
```


Let’s add some Mongo In:
Replace hardcoded rdd- 

```
val mongoUri = s"mongodb://localhost"
val hadoopConfig = new Configuration()

hadoopConfig.set("mongo.input.uri", s"$mongoUri/graph.data")
hadoopConfig.set("mongo.output.uri", s"$mongoUri/analysis.output")


val rdd = context.newAPIHadoopRDD(hadoopConfig, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object], classOf[BSONObject])
```

So, the really cool park about spark and graphx, is you can do neat munging. Our goal here is to create a graph of people that we can then analyze, but we’re starting with just a simple list of emails. Let’s start with creating our nodes

Couple of items – add a mapper:

```
object Analyzer {
  def parseObj(obj: BSONObject): Email = {
    val from = if(obj.containsField("From")) obj.get("From").asInstanceOf[BasicDBList].map(_.toString).toList else List()
    val to = if(obj.containsField("To")) obj.get("To").asInstanceOf[BasicDBList].map(_.toString).toList else List()
    val cc = if(obj.containsField("CC")) obj.get("CC").asInstanceOf[BasicDBList].map(_.toString).toList else List()
    val bcc = if(obj.containsField("BCC")) obj.get("BCC").asInstanceOf[BasicDBList].map(_.toString).toList else List()

    val subject = obj.get("Subject").toString
    val id = obj.get("_id").toString
    Email(id, subject, from, to, cc, bcc)
  }
}
```

Next, we’re going to simply create a  list of unique email addresses.


```
    val mapped = rdd.map(m => Analyzer.parseObj(m._2))

    val emailListRDD = mapped.flatMap(m => m.from ::: m.to ::: m.cc ::: m.bcc)
                             .distinct()
                             .zipWithIndex()
                             .map(m => (m._2, m._1))
```

Then, let's actually map this list to a graph
```
//Goes in the Analyzer Object
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


    val emailLookup = emailListRDD.collect().toMap
    val idLookup = emailLookup.map(m => (m._2, m._1)).toMap

    val edges = mapped.flatMap(m => Analyzer.parseEdge(m, idLookup))

    val g = Graph(emailListRDD, edges)

    Analyzer.outputGraph("MainGraph.gml", g)
```

Ok, now the magic happens – we’re going to use two out of the box graph analysis techniques with 

First, everyone here has heard of pagerank:

```
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
```


Let's look at strongly connected components - this is a method for community finding
```
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

```




Finally, let's write our own algorithm


First, we need some way to track messageing counts

```
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


    println(orderedCRS.sortBy(m => m._3).collect.mkString("\n"))

    println("------------")

    println(orderedCRS.sortBy(m => m._4, ascending = false).collect.mkString("\n"))
```
