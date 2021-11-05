// Package Information
name := "SparkApp"
organization := "main.scala"
version := "0.1"
scalaVersion := "2.12.9"

// Spark & Hadoop Information
val sparkVersion = "3.2.0"

// Resolvers allows us to include spark packages
resolvers += "bintray-spark-packages" at
  "https://dl.bintray.com/spark-packages/maven/"
resolvers += "Typesafe Simple Repository" at
  "https://repo.typesafe.com/typesafe/maven-releases/"
resolvers += "MavenRepository" at
  "https://mvnrepository.com/"

// Dependencies
libraryDependencies ++= Seq(
  // spark core
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  // snowflake
  "net.snowflake" %% "spark-snowflake" % "2.9.1-spark_3.1",
  "net.snowflake" % "snowflake-jdbc" % "3.13.6"
)
