name := "spark-cassandra-count"

version := "1.0"

scalaVersion := "2.11.11"

val sparkVersion = "2.2.0"

val SparkCassandraConnector = "2.0.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % SparkCassandraConnector % "provided"

resolvers += Resolver.sonatypeRepo("public")

logLevel := Level.Error

// We do this so that Spark Dependencies will not be bundled with our fat jar
// but will still be included on the classpath when we do a sbt/run
//run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

// assemblySettings
