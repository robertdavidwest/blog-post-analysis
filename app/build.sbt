name := "DE-pretest"
version := "0.1"
fork:= true

scalaVersion := "2.13.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.1",
  "org.apache.spark" %% "spark-sql" % "3.2.0",
  "org.apache.spark" %% "spark-hive" % "3.2.0",
  "org.scalatest" %% "scalatest" % "3.2.14" % Test,
)