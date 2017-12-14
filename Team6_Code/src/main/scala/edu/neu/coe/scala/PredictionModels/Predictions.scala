package edu.neu.coe.scala.PredictionModels

import edu.neu.coe.scala.DatabaseConnection.DBConnection
import edu.neu.coe.scala.PreProcessing.PreProcessing
import org.apache.log4j.Logger
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Level
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.SparkSession

/**
  * Created by Team6 on 11/20/17.
  * The class is used to build machine leraning predictive models: Linear Regression and Random Forest Regression.
  */


object PredictionModels extends App {

  // To hide the Spark console messages
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val sparkConf = new SparkConf().setAppName("ScalaFinalProject").setMaster("local[2]");
  val sparkContext = SparkContext.getOrCreate(sparkConf)
  val sparkSession = SparkSession.builder.appName("ScalaFinalProject").config("spark.master", "local").getOrCreate
  val sqlContext = new SQLContext(sparkContext)

  //DBConnection
  val dbConnection=new DBConnection()


  //Loading and Pre-Processing the data.
  //Pre-processed data is saved in MySQL
  val preProcessedData = new PreProcessing(sparkSession, sparkContext, sqlContext, dbConnection)


  //Prediction Models

  val dataLoading = sqlContext.read.format("jdbc").option("url",dbConnection.url).
    option("driver",dbConnection.driver).
    option("dbtable",dbConnection.dbTable).
    option("user",dbConnection.user).
    option("password",dbConnection.password).load()
  dataLoading.registerTempTable("Housing_Data")

  //Bringing in our data from database
  def loadData(sqlContext: SQLContext):DataFrame = {
    sqlContext.sql("""SELECT int(YearBuilt) YearBuilt,int(YearRemodelled) YearRemodelled,
                  int(NumberOfBedrooms) NumberOfBedrooms,double(NumberOfBathrooms) NumberOfBathrooms,
                  double(NumberOfFloors) NumberOfFloors,int(LivingArea) LivingArea,int(BasementExisting) BasementExisting,
                  int(FireplaceExisting) FireplaceExisting,int(PoolExisting) PoolExisting,int(GarageExisting) GarageExisting,
                  int(GarageCarCount) GarageCarCount,int(CloseToCommute) CloseToCommute,int(SalesCondition) SalesCondition,
                  int(ZipCode) ZipCode,double(SalesPrice) label
                  FROM Housing_Data """).na.drop()
  }

  def getAvgSalesPrice(sqlContext: SQLContext):DataFrame = {
    sqlContext.sql("""select avg(SalesPrice) avgSalesPrice from Housing_Data""").na.drop()
  }

  //Assembling all vectors to one
  val assembler = new VectorAssembler()
    .setInputCols(Array("YearBuilt", "YearRemodelled", "BasementExisting",
      "FireplaceExisting", "PoolExisting", "GarageExisting","CloseToCommute","SalesCondition","NumberOfBedrooms",
      "NumberOfBathrooms","NumberOfFloors","LivingArea","GarageCarCount","ZipCode"))
    .setOutputCol("features")


  //Linear Regression Pipeline
  def preppedLRPipeline():Pipeline = {
    val lr = new LinearRegression()
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .addGrid(lr.maxIter,Array(5))
      .build()
    val pipeline = new Pipeline()
      .setStages(Array(assembler, lr))
    pipeline
  }


  //Random Forest Regression Pipeline
  def preppedRFPipeline():Pipeline = {
    val dfr = new RandomForestRegressor()
    val paramGrid = new ParamGridBuilder()
      .addGrid(dfr.minInstancesPerNode, Array(1, 20))
      .addGrid(dfr.maxBins,Array(20))
      .addGrid(dfr.maxDepth, Array(20))
      .addGrid(dfr.numTrees, Array(20))
      .build()
    val pipeline = new Pipeline()
      .setStages(Array(assembler, dfr))
    pipeline
  }


  // Loading the data from MySQL database
  val data = loadData(sqlContext)
  //Splitting the data into training and test data in 80:20 ratio
  val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2))

  //Average Sales Price
  val avgSalesPrice =getAvgSalesPrice(sqlContext).select("avgSalesPrice").first().getDouble(0)

  //Linear Regression Model
  val lrModel = preppedLRPipeline().fit(trainingData)
  val lrPredictions = lrModel.transform(testData).select("prediction","label")

  //Linear Regression Metrics
  val lrRegressionMetrics = new RegressionMetrics(lrPredictions.rdd.map(x =>
    (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))

  println("Linear Regression Test Metrics")
  println("Test R^2 Coef:"+lrRegressionMetrics.r2)
  //R^2 = 0.5470
  println("Test RMSE:"+lrRegressionMetrics.rootMeanSquaredError/avgSalesPrice)
  //RMSE = 0.4789


  // Saving the Linear Regression model
  lrModel.write.overwrite().save("./PredictionModels/LinearModel")



  //Random Forest Regression Model
  val rfModel = preppedRFPipeline().fit(trainingData)
  val rfPredictions = rfModel.transform(testData).select("prediction","label")

  //Random Forest Regression Metrics
  val rfRegressionMetrics = new RegressionMetrics(rfPredictions.rdd.map(x =>
    (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))

  println("Random Forest Regression Test Metrics")
  println("Test R^2 Coef:"+rfRegressionMetrics.r2)
  //R^2 = 0.5679
  println("Test RMSE:"+rfRegressionMetrics.rootMeanSquaredError/avgSalesPrice)
  //RMSE = 0.4677

  // Saving the Random Forest Regression model
  rfModel.write.overwrite().save("./PredictionModels/RandomForestModel")


  sparkSession.stop()

}


