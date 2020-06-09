name := "final-project"

version := "0.1"

scalaVersion := "2.12.11"

val sparkVersion = "2.4.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
)