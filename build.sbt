name := "FinalProject_Team6_Code"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.1.1",
  "org.apache.spark" %% "spark-hive" % "2.1.1",
  "org.apache.spark" %% "spark-mllib" % "2.1.1",
  "org.apache.commons" % "commons-lang3" % "3.0",
  "mysql" % "mysql-connector-java" % "5.1.12"
)

        