scalaVersion := "2.12.15"

name := "DimPipelineLoader"
version := "0.1.0-SNAPSHOT"
val sparkVersion = "3.1.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",  
  "org.apache.spark" %% "spark-sql" % sparkVersion,                
  "com.typesafe" % "config" % "1.3.1",
  "com.novocode" % "junit-interface" % "0.11" % "test",
  "junit" % "junit" % "4.12" % "test",
  "org.scalatest" %% "scalatest" % "3.2.12" % Test  
)


