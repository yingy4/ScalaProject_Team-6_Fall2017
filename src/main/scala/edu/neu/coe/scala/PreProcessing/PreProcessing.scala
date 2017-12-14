package edu.neu.coe.scala.PreProcessing

import edu.neu.coe.scala.DatabaseConnection.DBConnection
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import java.util.Properties
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import scala.util.Random

/**
  * Created by Team6 on 11/13/17.
  * The class is used to load and pre-process the data.
  */

class PreProcessing (sparkSession:SparkSession,sparkContext:SparkContext,sqlContext:SQLContext,dbConnection:DBConnection)
{


  //Method to read data from .csv files
  def getRawdata(sparkSession: SparkSession,filePath: String) = {
    val rawdata =sparkSession.read.option("header", "true").option("inferSchema", "true").csv(filePath)
    rawdata
  }

  //Reading the data
  var kcHouseData = getRawdata(sparkSession,"./data/Raw/kc_house_data.csv")
  var kaggleHouseData = getRawdata(sparkSession,"./data/Raw/kaggle_house_data.csv")
  var zipcodeData = getRawdata(sparkSession,"./data/Raw/zipcode_data.csv")
  val statesData = getRawdata(sparkSession,"./data/Raw/states_data.csv")


  //Pre-processing the kaggle_house_data

  //Removing unwanted columns
  //Method to remove unwanted columns
  def removeColumns(columnsToRemove: Seq[String], ds: Dataset[Row]): Dataset[Row] =
        ds.select(ds.columns.filter(colName => !columnsToRemove.contains(colName)).map(colName => new Column(colName)): _*)

  val kaggleColmumnsToRemove = Seq("MSSubClass", "MSZoning", "LotFrontage", "LotArea", "Street", "Alley", "LotShape", "LandContour", "Utilities",
        "LotConfig", "LandSlope", "Neighborhood", "Condition1", "Condition2", "BldgType", "OverallQual", "OverallCond", "RoofStyle", "RoofMatl", "Exterior1st",
        "Exterior2nd", "MasVnrType", "MasVnrArea", "ExterQual", "ExterCond", "Foundation", "BsmtQual", "BsmtCond", "BsmtExposure", "BsmtFinType1",
        "BsmtFinSF1", "BsmtFinType2", "BsmtFinSF2", "BsmtUnfSF", "Heating", "HeatingQC", "CentralAir", "Electrical", "1stFlrSF", "2ndFlrSF", "LowQualFinSF",
        "KitchenAbvGr", "KitchenQual", "TotRmsAbvGrd", "Functional", "FireplaceQu", "GarageType", "GarageYrBlt", "GarageFinish", "GarageCars", "GarageArea",
        "GarageQual", "GarageCond", "PavedDrive", "WoodDeckSF", "OpenPorchSF", "EnclosedPorch", "3SsnPorch", "ScreenPorch", "PoolQC", "Fence",
        "MiscFeature", "MiscVal", "MoSold", "YrSold", "SaleType"
      )

  kaggleHouseData = removeColumns(kaggleColmumnsToRemove, kaggleHouseData)

  //HouseStyle has values like "2Story","1.5Fin"... We will convert it to number of floors(1,2..)
  import org.apache.spark.sql.functions._ // for `when`
  kaggleHouseData = kaggleHouseData.withColumn("Floors", expr("case when HouseStyle=='1Story' then 1 " +
        "when HouseStyle in ('1.5Fin','1.5Unf') then 1.5 " +
        "when HouseStyle=='2Story' then 2 " +
        " when HouseStyle in ('2.5Fin','2.5Unf') then 2.5 else 3 end"))
  kaggleHouseData = kaggleHouseData.drop("HouseStyle") //Removing the original column

  //TotalBsmtSF has the basement in sq.ft. We are keeping only the data like whether house is having basement or not (0 or 1).
  import sqlContext.implicits._ // for  $""
  kaggleHouseData = kaggleHouseData.withColumn("BasementExisting", when($"TotalBsmtSF" === "0", 0).otherwise(1))
  kaggleHouseData = kaggleHouseData.drop("TotalBsmtSF") //Removing the original column


  //Making NumberOfBathrooms=BsmtFullBath+BsmtHalfBath+FullBath+HalfBath
  kaggleHouseData = kaggleHouseData.withColumn("NumberOfBathrooms", expr("BsmtFullBath+(0.5*BsmtHalfBath)+FullBath+(0.5*HalfBath)"))
  kaggleHouseData = removeColumns(Seq("BsmtFullBath", "BsmtHalfBath", "FullBath", "HalfBath"), kaggleHouseData) //Removing the oringinal 4 columns

  //Fireplaces has the count of fireplace. We are keeping only the data like whether house is having firplace or not (0 or 1).
  kaggleHouseData = kaggleHouseData.withColumn("FireplaceExisting", when($"Fireplaces" === "0", 0).otherwise(1))
  kaggleHouseData = kaggleHouseData.drop("Fireplaces") //Removing the original column

  //PoolArea has the area of swimming pool. We are keeping only the data like whether house is having swimming pool or not (0 or 1).
  kaggleHouseData = kaggleHouseData.withColumn("PoolExisting", when($"PoolArea" === "0", 0).otherwise(1))
  kaggleHouseData = kaggleHouseData.drop("PoolArea") //Removing the original column

  //SaleCondition is having data as "Abnormal","Family"...We are converting this data in the range from 1 to 5.this is needed for regression.
  kaggleHouseData = kaggleHouseData.withColumn("SalesCondition", expr("case when SaleCondition in ('Abnormal','AdjLand') then 1" +
      " when SaleCondition='Alloca' then 2 when SaleCondition='Family' then 3 when SaleCondition='Normal' then 4  else 5 end"))
  kaggleHouseData = kaggleHouseData.drop("SaleCondition") //Removing the original column


  //Simulating the data for CloseToCommute column
  def generateBooleanData(ds: Dataset[Row], newColumnName: String, joinColumnName: String): Dataset[Row] = {
    val listBoolean = for (i <- 1 to ds.count().toInt) yield if (Random.nextBoolean() == true) 1 else 0
    //Creating a list of random Boolean values
    val newDS = listBoolean.toDF(newColumnName).withColumn(joinColumnName, (monotonicallyIncreasingId() + 1)) // Creating a dataframe of Boolean values generated with common column "Id"
    ds.join(newDS, joinColumnName)
  }
  kaggleHouseData = generateBooleanData(kaggleHouseData, "CloseToCommute", "Id")

  //Method to rename existing columns
  def renameExistingColumns(newColumnNames: Seq[String], ds: Dataset[Row]): Dataset[Row] =
        ds.toDF(newColumnNames: _*) //Renaming

  val newKaggleColumnnames = Seq("Id", "YearBuilt", "YearRemodelled", "LivingArea", "NumberOfBedrooms", "SalesPrice", "GarageCarCount",
        "GarageExisting", "ZipCode", "NumberOfFloors", "BasementExisting", "NumberOfBathrooms", "FireplaceExisting", "PoolExisting", "SalesCondition",
        "CloseToCommute")
  kaggleHouseData = renameExistingColumns(newKaggleColumnnames, kaggleHouseData)

  kaggleHouseData = kaggleHouseData.select("Id", "YearBuilt", "YearRemodelled", "NumberOfBedrooms", "NumberOfBathrooms", "NumberOfFloors",
        "LivingArea", "BasementExisting", "FireplaceExisting", "PoolExisting", "GarageExisting", "GarageCarCount", "CloseToCommute", "SalesCondition",
        "ZipCode", "SalesPrice")


  //Pre-processing the KC_house_data


  //Removing unwanted columns
  val kcColmumnsToRemove = Seq("date", "sqft_lot", "waterfront", "view", "grade", "sqft_above", "lat", "long", "sqft_living15",
        "sqft_lot15")
  kcHouseData = removeColumns(kcColmumnsToRemove, kcHouseData)

  //TotalBsmtSF has the basement in sq.ft. We are keeping only the data like whether house is having basement or not (0 or 1).
  kcHouseData = kcHouseData.withColumn("BasementExisting", when($"sqft_basement" === "0", 0).otherwise(1))
  kcHouseData = kcHouseData.drop("sqft_basement") //Removing the original column

  //Simulating the data for CloseToCommute,FireplaceExisting,PoolExisting columns
  kcHouseData = generateBooleanData(kcHouseData, "CloseToCommute", "Id")
  kcHouseData = generateBooleanData(kcHouseData, "FireplaceExisting", "Id")
  kcHouseData = generateBooleanData(kcHouseData, "PoolExisting", "Id")

  //Renaming the existing columns
  val newkcColumnnames = Seq("Id", "SalesPrice", "NumberOfBedrooms", "NumberOfBathrooms", "GarageCarCount", "GarageExisting", "LivingArea", "NumberOfFloors", "SalesCondition", "YearBuilt", "YearRemodelled",
        "ZipCode", "BasementExisting", "CloseToCommute", "FireplaceExisting", "PoolExisting")
  kcHouseData = renameExistingColumns(newkcColumnnames, kcHouseData)

  kcHouseData = kcHouseData.select("Id", "YearBuilt", "YearRemodelled", "NumberOfBedrooms", "NumberOfBathrooms", "NumberOfFloors",
        "LivingArea", "BasementExisting", "FireplaceExisting", "PoolExisting", "GarageExisting", "GarageCarCount", "CloseToCommute", "SalesCondition",
        "ZipCode", "SalesPrice")




  //Merging Kaggle House Data and KC House Data
  var combinedData = kaggleHouseData.union(kcHouseData)



  //Pre-processing ZipCode data
  //Removing the unwanted columns
  val zipcodeColmumnsToRemove = Seq("Id", "ZipCodeType", "LocationType", "Lat", "Long", "Xaxis", "Yaxis", "Zaxis",
        "WorldRegion", "Country", "LocationText", "Location", "Decommisioned", "TaxReturnsFiled", "EstimatedPopulation", "TotalWages", "Notes"
      )
  zipcodeData = removeColumns(zipcodeColmumnsToRemove, zipcodeData)


  //Joining ZipCode Data with States Data for having state's full name based on StateAbbreviation
  zipcodeData = zipcodeData.join(statesData, zipcodeData.col("StateAbbreviation") === statesData.col("Abbreviation"))
  zipcodeData = zipcodeData.drop("Abbreviation")



  //Joining ZipCode Data with combined Kaggle and  KC housing Data based on ZipCode

  combinedData = combinedData.join(zipcodeData, "ZipCode")



  //Id column will now be having duplicate records because Id in both Kaggle and KC Housing data are starting from 1.
  // So we are trying to remove make it continuous integers from 1 to n
  combinedData = combinedData.withColumn("NewId", functions.row_number().over(Window.orderBy("Id")))
  combinedData = combinedData.drop("Id")
  combinedData = combinedData.withColumnRenamed("NewId", "Id")

  //Re-ordering the columns
  combinedData = combinedData.select("Id", "YearBuilt", "YearRemodelled", "NumberOfBedrooms", "NumberOfBathrooms", "NumberOfFloors",
        "LivingArea", "BasementExisting", "FireplaceExisting", "PoolExisting", "GarageExisting", "GarageCarCount", "CloseToCommute", "SalesCondition",
        "ZipCode", "City", "StateAbbreviation", "State", "SalesPrice")
  combinedData = combinedData.orderBy("Id")



  // Sales Price is a non-nullable column
  //Replacing "NaN" and "Null" values with mean for SalesPrice column
  import org.apache.spark.sql.functions.mean
  combinedData = combinedData.na.fill(combinedData.select(mean("SalesPrice")).first()(0).asInstanceOf[Double], Seq("SalesPrice"))


  //Saving the final data to MySQL table

  //create properties object
  val prop = new Properties()
  prop.setProperty("driver", dbConnection.driver)
  prop.setProperty("user", dbConnection.user)
  prop.setProperty("password", dbConnection.password)


  // /write data from spark dataframe to database
  combinedData.write.mode("Overwrite").jdbc(dbConnection.url, dbConnection.dbTable, prop)
  //By default Spark creates and inserts data into the destination table.
  // If the table already exists, we will get a TableAlreadyExists Exception.
  // To avoid this exception, mode("Overwrite") is used;
  // It will create and insert if table is not existing; if existing it will overwrite the existing table.

    }
