name := "drone-project"

version := "0.1"

scalaVersion := "2.11.8"
//fork in run := true
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.7.0"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.0"
libraryDependencies += "net.liftweb" %%"lift-json" % "3.4.3"
libraryDependencies += "com.google.code.gson" % "gson" % "2.8.6"
libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.7"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"