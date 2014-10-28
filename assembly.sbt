import AssemblyKeys._

// put this at the top of the file

assemblySettings

test in assembly := {}

excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
  cp.filter(m => {
    m.data.getName == "spark-assembly-1.1.0-hadoop2.4.0.jar" ||
      m.data.getName == "akka-remote_2.10-2.2.3-shaded-protobuf.jar" ||
      m.data.getName == "akka-actor_2.10-2.2.3-shaded-protobuf.jar"
  })
}