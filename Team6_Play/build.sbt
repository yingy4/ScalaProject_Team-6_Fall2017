name := """Team6_Play"""

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  cache,
  ws,
  specs2 % Test,
  "mysql" % "mysql-connector-java" % "5.1.34",
  "com.typesafe.play" %% "play-slick" % "1.1.0",
  "com.typesafe.play" %% "play-slick-evolutions" % "1.1.0",
  "org.apache.spark" %% "spark-core" % "2.1.1",
  "org.apache.spark" %% "spark-sql" % "2.1.1",
  "org.apache.spark" %% "spark-hive" % "2.1.1",
  "org.apache.spark" %% "spark-mllib" % "2.1.1"
)

parallelExecution in Test := false


resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"

// Play provides two styles of routers, one expects its actions to be injected, the
// other, legacy style, accesses its actions statically.
routesGenerator := InjectedRoutesGenerator
