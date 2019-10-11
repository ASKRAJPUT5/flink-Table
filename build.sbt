name := "flinkTable25"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % "1.9.0"

libraryDependencies += "org.apache.flink" % "flink-table" % "1.9.0"

libraryDependencies += "org.apache.flink" %% "flink-connector-kafka-0.10" % "1.9.0"

libraryDependencies += "org.apache.flink" %% "flink-scala" % "1.9.0"

libraryDependencies += "org.apache.flink" %% "flink-table-api-scala-bridge" % "1.9.0"

libraryDependencies += "org.apache.flink" %% "flink-table-planner" % "1.9.0"
