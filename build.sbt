name := "ParticulateMatter"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.1"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.2.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.1"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.2.1"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.4"
libraryDependencies += "org.apache.kafka" %% "kafka" % "0.10.0.1" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"