package edu.neu.coe.scala

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
* Created by Team6 on 11/22/17.
* The class is used to load the test data from MySQL database.
*/

  abstract class PreProcessingSpec extends FlatSpec with BeforeAndAfterAll with Matchers {
    private val master = "local"
    private val appName = "Testing Sales Price Prediction"
    var sc: SparkContext = _
    var sqlContext: SQLContext = _

    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
      .set("spark.driver.allowMultipleContexts","true")


    override protected def beforeAll(): Unit = {
      super.beforeAll()
      sc = new SparkContext(conf)
      sqlContext = new SQLContext(sc)
    }

    override protected def afterAll(): Unit = {
      try {
        sc.stop()
        sc = null
        sqlContext = null
      } finally {
        super.afterAll()
      }
    }


  //Loading and Pre-Processing the data.
  //Pre-processed data is saved in MySQL
//  val preProcessedData = new PreProcessing(sparkSession, sparkContext, sqlContext, dbConnectionTest)


  //Prediction Models

  //Bringing in our data from database
  def loadData(sqlContext: SQLContext):DataFrame = {
    val url = "jdbc:mysql://localhost:3306/scala_project"
    val driver = "com.mysql.jdbc.Driver"
    val dbTable = "housing"
    val user = "root"
    val password = "Infy123+"
    val dataTest = sqlContext.read.format("jdbc").option("url",url).
      option("driver",driver).
      option("dbtable",dbTable).
      option("user",user).
      option("password",password).load()

    dataTest.createOrReplaceTempView("Housing")

    sqlContext.sql("""SELECT int(YearBuilt) YearBuilt,int(YearRemodelled) YearRemodelled,
                  int(NumberOfBedrooms) NumberOfBedrooms,double(NumberOfBathrooms) NumberOfBathrooms,
                  double(NumberOfFloors) NumberOfFloors,int(LivingArea) LivingArea,int(BasementExisting) BasementExisting,
                  int(FireplaceExisting) FireplaceExisting,int(PoolExisting) PoolExisting,int(GarageExisting) GarageExisting,
                  int(GarageCarCount) GarageCarCount,int(CloseToCommute) CloseToCommute,int(SalesCondition) SalesCondition,
                  City,State,double(SalesPrice) label
                  FROM Housing limit 100""").na.drop()
  }}

