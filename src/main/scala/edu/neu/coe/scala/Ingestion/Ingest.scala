package edu.neu.coe.scala.ingest

import scala.io.Source
import scala.util.Try


/**
  * Created by Team6 on 11/14/2017.
  */


object Ingest {

  /** Generic function to load in CSV */

  def readcsv(csv: String): List[List[String]] = {
    val src = Source.fromFile(csv)
    src.getLines.map(x => x.split(",").toList).toList
  }

  /*def priceToId2Map(csvLocation: String , Ids: List[String]) : Map[Int, String] = {
    val csv = readcsv(csvLocation)
    csv.drop(1)
      .filter(x=> Ids.contains(x(1).split(" ").head))
      .map(x => (x(0).toInt, x(1).split(" ").head))
      .toMap
  }

  def recordToLabel2Map(csvLocation: String): Map[String, List[Int]] = {
    val csv = readcsv(csvLocation)
    csv.drop(1).map(x => (x(0), (x(1).split(" ").map(_.toInt)).toList)).toMap
  }

  def getUniqueIDForTest(csvLocation: String): List[String] = {
    val csv = readcsv(csvLocation)
    csv.drop(1).map(x => (x(0))).toList
  }*/
}
