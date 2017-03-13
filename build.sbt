name := "spark-streaming-kafka"

version := "1.0"

scalaVersion := "2.11.8"


libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.0.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.0.0",
//  "org.apache.spark" % "spark-streaming-kafka-0-8-assembly_2.11" % "2.0.0",
  "org.apache.spark" % "spark-streaming_2.11" % "2.0.0",
  "org.apache.spark" % "spark-core_2.11" % "2.0.0",
  "org.scala-lang" % "scala-compiler" % "2.11.8",
  "org.scala-lang.modules" % "scala-parser-combinators_2.11" % "1.0.4",
  "org.scala-lang.modules" % "scala-xml_2.11" % "1.0.4"
)

