name := "SparkTask"

version := "0.1"

scalaVersion := "2.11.8"

val scalaSuffixVersion = "2.11"

val sparkVersion = "2.3.0"

val sparkDependencies = Seq(
  "org.apache.spark" % s"spark-core_$scalaSuffixVersion" % sparkVersion,
  "org.apache.spark" % s"spark-sql_$scalaSuffixVersion" % sparkVersion
)

val utilDependencies = Seq(
  "org.json4s" % s"json4s-native_$scalaSuffixVersion" % "3.2.11"
)

val testDependencies = Seq(
  "org.scalatest" % s"scalatest_$scalaSuffixVersion" % "3.0.3"
)

libraryDependencies ++= sparkDependencies ++ utilDependencies ++ testDependencies.map(_ % "test")
