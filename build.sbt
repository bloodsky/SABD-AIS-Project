name := "SABD-AIS-Project"

version := "0.1"

scalaVersion := "2.12.14"

// https://mvnrepository.com/artifact/org.apache.flink/flink-core
libraryDependencies += "org.apache.flink" % "flink-core" % "1.12.1"
// https://mvnrepository.com/artifact/org.apache.flink/flink-streaming-scala
libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % "1.12.1" % "provided"
// https://mvnrepository.com/artifact/org.apache.flink/flink-clients
libraryDependencies += "org.apache.flink" %% "flink-clients" % "1.12.1"
// https://mvnrepository.com/artifact/org.apache.flink/flink-metrics-dropwizard
libraryDependencies += "org.apache.flink" % "flink-metrics-dropwizard" % "1.12.1"
