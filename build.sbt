name := "kafka-example"

version := "1.0"

scalaVersion := "2.12.3"
val kafkaVersion = "0.9.0.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % kafkaVersion
resolvers += Resolver.mavenLocal
        