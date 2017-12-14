import sbt.Keys.libraryDependencies

name := "Team6_Code"

version := "0.1"

scalaVersion := "2.11.8"

val scalaTestVersion = "2.2.4"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"


libraryDependencies ++= Seq(

  "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.4.1",
  "org.apache.spark" %% "spark-core" % "2.1.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.1.1",
  "org.apache.spark" %% "spark-hive" % "2.1.1",
  "org.apache.spark" %% "spark-mllib" % "2.1.1",
  "org.apache.commons" % "commons-lang3" % "3.0",
  "mysql" % "mysql-connector-java" % "5.1.12",
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
  "org.ccil.cowan.tagsoup" % "tagsoup" % "1.2.1"

)
//libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.4"
//libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"

  

