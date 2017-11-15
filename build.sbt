name := "CSYE7200_FinalProject_Team6_Fall2017"

version := "0.1"

scalaVersion := "2.11.8"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0" % "provided",
  "org.deeplearning4j" % "deeplearning4j-core" % "0.4-rc3.8",
  "org.imgscalr" % "imgscalr-lib" % "4.2",
  "com.sksamuel.scrimage" %% "scrimage-core" % "2.1.0",
  "com.sksamuel.scrimage" %% "scrimage-io-extra" % "2.1.0",
  "com.sksamuel.scrimage" %% "scrimage-filters" % "2.1.0",
  "org.deeplearning4j" % "deeplearning4j-ui_2.11" % "0.8.0",
  "org.nd4j" % "nd4j-native-platform" % "0.8.0",
  "org.nd4j" % "nd4s_2.11" % "0.8.0",
  "org.nd4j" % "nd4j-backends" % "0.8.0"

)

        