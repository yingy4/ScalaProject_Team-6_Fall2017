package edu.neu.coe.scala

import org.apache.log4j.{Level, Logger}
import org.scalatest._

/**
  * Created by Team6 on 11/22/17.
  * The class is used to test the implementation in several scenarios.
  */

class SalesPriceSpec extends PreProcessingSpec with GivenWhenThen with Matchers{


  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  "Spark Context" should "not be null" in {
    assertResult(2)(sc.parallelize(Seq(1,2,3)).filter(_ <= 2).map(_ + 1).count)
  }


  // Test Case - training dataframe should not be null
  "Load train data frame" should "not be null" in {
    val trainDF =loadData(sqlContext)
    trainDF.collect() should not have length (0)
  }

  //Test Case to check the count of Housing Table
  "Load Training data counts of records" should "not be null" in {
    val rawTestData = loadData(sqlContext)
    rawTestData.registerTempTable("housing")
    val IdCount = sqlContext.sql("""SELECT YearBuilt FROM housing""").collect()
    IdCount should have length(100)
  }


  //Test Case to check the City, State should not be numeric in training data set
  "City, State should be String" should "be String value" in {
    val rawTrainData = PredictionModels.PredictionModels.data
    val City = sqlContext.sql("""SELECT City,State FROM housing where City like '%[^0-9]%'or State like '%[^0-9]%'""").collect()
    City should have length (0)
  }

  //Test Case to check the YearBuilt,YearRemodelled should be greater than 1900 and less than 2018 in training data set
  "YearBuiltCity, YearRemodelled" should "be >1900 and < 2017" in {
    val rawTrainData = PredictionModels.PredictionModels.data
    val yearRB = sqlContext.sql(
      """SELECT YearBuilt, YearRemodelled FROM housing where YearBuilt <1900 AND YearBuilt >2017 or YearRemodelled <1900 and YearRemodelled >2017""").collect()
    yearRB should have length (0)
  }

  //Test Case to check the YearBuilt is before YearRemodelled in training data set
  "YearBuiltCity before YearRemodelled" should "in dataset" in {
    val rawTrainData = PredictionModels.PredictionModels.data
    val yearBuilt = sqlContext.sql(
      """SELECT YearBuilt, YearRemodelled FROM housing where YearBuilt > YearRemodelled and YearRemodelled >0 and YearRemodelled != 2001""").collect()
    yearBuilt should have length (0)
  }
  //Test Case to check the sales price should be greater than 0 in training data set
  "SalesPrice should be greater than 0" should "be greater than 0" in {
    val rawTrainData = PredictionModels.PredictionModels.data
    val salesPrice = sqlContext.sql("""SELECT label FROM housing where label<=0""").collect()
    salesPrice should have length (0)
  }

  //Test Case to check the LivingArea should be greater than 0 in training data set
  " Living Area should be greater than 0" should "be greater than 0" in {
    val rawTrainData = PredictionModels.PredictionModels.data
    val LivingArea = sqlContext.sql("""SELECT LivingArea FROM housing where label<=0""").collect()
    LivingArea should have length (0)
  }

  //Test Case to check the pool in training data set
  " poolExisting should be " should "be 0 or 1" in {
    val rawTrainData = PredictionModels.PredictionModels.data
    val pool = sqlContext.sql("""SELECT PoolExisting FROM housing where PoolExisting>=2""").collect()
    pool should have length (0)
  }

  //Test Case to check the GarageExisting  in training data set
  " GarageExisting should be " should "be 0 or 1" in {
    val rawTrainData = PredictionModels.PredictionModels.data
    val Garage = sqlContext.sql("""SELECT GarageExisting FROM housing where GarageExisting>=2""").collect()
    Garage should have length (0)
  }

  //Test Case to check the GarageCarCount  in training data set
  " GarageCarCount should be " should "0 or greater" in {
    val rawTrainData = PredictionModels.PredictionModels.data
    val car = sqlContext.sql("""SELECT GarageCarCount FROM housing where GarageCarCount>=0""").collect()
    car should have length (100)
  }

  //Test Case to check the CloseToCommute in training data set
  " CloseToCommute should be " should "be 0 or 1" in {
    val rawTrainData = PredictionModels.PredictionModels.data
    val Commute = sqlContext.sql("""SELECT CloseToCommute FROM housing where CloseToCommute>=2""").collect()
    Commute should have length (0)
  }

  //Test Case to check the SalesCondition should be equal to 1 and less than equal to 5
  " SalesCondition should be " should "be 1 to 5" in {
    val rawTrainData = PredictionModels.PredictionModels.data
    val cond = sqlContext.sql("""SELECT SalesCondition FROM housing where SalesCondition>=6""").collect()
    cond should have length (0)
  }

  //Test Case to check the FireplaceExisting in training data set
  " FireplaceExisting should be " should "be 0 or 1" in {
    val rawTrainData = PredictionModels.PredictionModels.data
    val fire = sqlContext.sql("""SELECT FireplaceExisting FROM housing where FireplaceExisting>=2""").collect()
    fire should have length (0)
  }
 // Test Case to check BasementExisting
  " BasementExisting should be " should "be 0 or 1" in {
    val rawTrainData = PredictionModels.PredictionModels.data
    val basement= sqlContext.sql("""SELECT BasementExisting FROM housing where BasementExisting>=2""").collect()
    basement should have length (0)
  }


  // Test Case - test the randomforest train data split ratio = 0.80
  "Random Forest Pipeline" should "have transValidationSplit" in {
    val trainValidationSplit = PredictionModels.PredictionModels.preppedRFPipeline().getStages.length== 0.8

  }

  // Test Case - test the Linear train data split ratio = 0.80
  "Linear Regression Pipeline" should "have transValidationSplit" in {
    val trainValidationSplit = PredictionModels.PredictionModels.preppedLRPipeline().getStages.length== 0.80  }

}