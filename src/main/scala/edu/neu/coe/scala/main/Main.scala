
package edu.neu.coe.scala.main

import edu.neu.coe.scala.ingest.Ingest

/**
  * Created by 
  */
object Main extends App {

  val kaggleHouseData = Ingest.readcsv("./data/Raw/kaggle_house_data.csv")
  val kcHouseData = Ingest.readcsv("./data/Raw/kc_house_data.csv")
  val zipcodeData = Ingest.readcsv(("./data/Raw/zipcode_data.csv"))

  println("Kaggle data size: " + kaggleHouseData.size)
  println("KC data size: " + kcHouseData.size)
  println("Zipcode data size: " + zipcodeData.size)

}
