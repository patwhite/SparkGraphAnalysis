name := """SparkGraphAnalysis"""

version := "1.0"

scalaVersion := "2.10.4"

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.2.4",
  "com.google.apis" % "google-api-services-gmail" % "v1-rev15-1.19.0",
  "org.reactivemongo" %% "reactivemongo" % "0.10.5.0.akka22"
)
  