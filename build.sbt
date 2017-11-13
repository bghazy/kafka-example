name := "kafka-example"

version := "1.0"

scalaVersion := "2.12.3"

val kafkaVersion = "0.10.2.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % kafkaVersion
libraryDependencies += "org.apache.kafka" % "kafka-streams" % kafkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-http" % "10.0.10"

resolvers += Resolver.mavenLocal
