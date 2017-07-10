
//val sparkVersion = "1.6.2" ;
val sparkVersion = "2.1.1";

lazy val root=(project in file(".")).
  settings(
    name := "hello",
    version := "1.0",
    //scalaVersion := "2.10.5"
    scalaVersion := "2.11.8"
    
    )
  //"org.postgresql" % "postgresql" % "42.1.1",
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" %  sparkVersion  % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion  ,
  "org.apache.spark" %% "spark-hive" % sparkVersion  ,
  "org.apache.spark" %% "spark-streaming" % sparkVersion  ,
  "org.scalacheck" %% "scalacheck" % "1.13.2" % "test",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "org.scalactic" %% "scalactic" % "2.2.6",
  "org.slf4j" % "slf4j-api" % "1.7.5",
  "org.slf4j" % "slf4j-simple" % "1.7.5",
  "org.skife.com.typesafe.config" % "typesafe-config" % "0.3.0"
)
