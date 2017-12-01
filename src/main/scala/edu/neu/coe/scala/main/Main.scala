
package edu.neu.coe.scala.main

import java.util.Properties

import org.apache.spark.sql._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import edu.neu.coe.scala.ingest.Ingest
import edu.neu.coe.scala.main.Main.{combinedData, sparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.util.Random


/**
  * Created by 
  */
object Main extends App {

  // To hide the Spark console messages
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val sparkConf = new SparkConf().setAppName("ScalaFinalProject").setMaster("local[2]");
  val sparkContext= new SparkContext(sparkConf)
  val sparkSession = SparkSession.builder.appName("ScalaFinalProject").config("spark.master", "local").getOrCreate
  //Spark Context is automatically created when Spark Session has been created. But SparkContext was required to import implicits

 //Loading the raw data from .csv files
  var kaggleHouseData = sparkSession.read.option("header","true").option("inferSchema","true").csv("./data/Raw/kaggle_house_data.csv")
  var kcHouseData = sparkSession.read.option("header","true").option("inferSchema","true").csv("./data/Raw/kc_house_data.csv")
  var zipcodeData = sparkSession.read.option("header","true").option("inferSchema","true").csv("./data/Raw/zipcode_data.csv")
  val statesData = sparkSession.read.option("header","true").option("inferSchema","true").csv("./data/Raw/states_data.csv")
  println("Kaggle data size: " + kaggleHouseData.count())
  println("KC data size: " + kcHouseData.count())
  println("Zipcode data size: " + zipcodeData.count())
  println("States data size: " + statesData.count())


  //Pre-processing the kaggle_house_data

  //Removing unwanted columns
  //Method to remove unwanted columns
  def removeColumns(columnsToRemove:Seq[String], ds: Dataset[Row]): Dataset[Row] =
     ds.select(ds.columns .filter(colName => !columnsToRemove.contains(colName)) .map(colName => new Column(colName)): _*)

  val kaggleColmumnsToRemove=Seq("MSSubClass","MSZoning","LotFrontage","LotArea","Street","Alley","LotShape","LandContour","Utilities",
    "LotConfig","LandSlope","Neighborhood","Condition1","Condition2","BldgType","OverallQual","OverallCond","RoofStyle","RoofMatl","Exterior1st",
    "Exterior2nd", "MasVnrType","MasVnrArea","ExterQual","ExterCond","Foundation","BsmtQual","BsmtCond","BsmtExposure","BsmtFinType1",
    "BsmtFinSF1", "BsmtFinType2","BsmtFinSF2", "BsmtUnfSF","Heating","HeatingQC","CentralAir","Electrical","1stFlrSF","2ndFlrSF","LowQualFinSF",
    "KitchenAbvGr", "KitchenQual","TotRmsAbvGrd","Functional", "FireplaceQu","GarageType","GarageYrBlt","GarageFinish","GarageCars","GarageArea",
    "GarageQual", "GarageCond","PavedDrive","WoodDeckSF","OpenPorchSF", "EnclosedPorch", "3SsnPorch","ScreenPorch","PoolQC","Fence",
    "MiscFeature","MiscVal", "MoSold","YrSold","SaleType"
  )

  kaggleHouseData = removeColumns(kaggleColmumnsToRemove,kaggleHouseData)

  //HouseStyle has values like "2Story","1.5Fin"... We will convert it to number of floors(1,2..)
  val sqlContext = new SQLContext(sparkContext) // to import sqlContext.implicits
  import org.apache.spark.sql.functions._ // for `when`
  kaggleHouseData = kaggleHouseData.withColumn("Floors", expr("case when HouseStyle=='1Story' then 1 " +
    "when HouseStyle in ('1.5Fin','1.5Unf') then 1.5 " +
    "when HouseStyle=='2Story' then 2 "+
    " when HouseStyle in ('2.5Fin','2.5Unf') then 2.5 else 3 end"))
  kaggleHouseData=kaggleHouseData.drop("HouseStyle")//Removing the original column

  //TotalBsmtSF has the basement in sq.ft. We are keeping only the data like whether house is having basement or not (0 or 1).
  import sqlContext.implicits._ // for  $""
  kaggleHouseData = kaggleHouseData.withColumn("BasementExisting", when($"TotalBsmtSF" === "0", 0).otherwise(1))
  kaggleHouseData=kaggleHouseData.drop("TotalBsmtSF")//Removing the original column


  //Making NumberOfBathrooms=BsmtFullBath+BsmtHalfBath+FullBath+HalfBath
 // val expr="BsmtFullBath+BsmtHalfBath+FullBath+HalfBath"
  kaggleHouseData=kaggleHouseData.withColumn("NumberOfBathrooms",expr("BsmtFullBath+(0.5*BsmtHalfBath)+FullBath+(0.5*HalfBath)"))
  kaggleHouseData=removeColumns(Seq("BsmtFullBath","BsmtHalfBath","FullBath","HalfBath"),kaggleHouseData)//Removing the oringinal 4 columns

  //Fireplaces has the count of fireplace. We are keeping only the data like whether house is having firplace or not (0 or 1).
  kaggleHouseData = kaggleHouseData.withColumn("FireplaceExisting", when($"Fireplaces" === "0", 0).otherwise(1))
  kaggleHouseData=kaggleHouseData.drop("Fireplaces")//Removing the original column

  //PoolArea has the are of swimming pool. We are keeping only the data like whether house is having swimming pool or not (0 or 1).
  kaggleHouseData = kaggleHouseData.withColumn("PoolExisting", when($"PoolArea" === "0", 0).otherwise(1))
  kaggleHouseData=kaggleHouseData.drop("PoolArea")//Removing the original column

  //SaleCondition is having data as "Abnormal","Family"...We are converting this data in the range from 1 to 5.this is needed for regression.
  kaggleHouseData = kaggleHouseData.withColumn("SalesCondition", expr("case when SaleCondition in ('Abnormal','AdjLand') then 1" +
    " when SaleCondition='Alloca' then 2 when SaleCondition='Family' then 3 when SaleCondition='Normal' then 4  else 5 end"))
  kaggleHouseData=kaggleHouseData.drop("SaleCondition")//Removing the original column



  //Simulating the data for CloseToCommute column
  def generateBooleanData(ds: Dataset[Row],newColumnName:String,joinColumnName:String):Dataset[Row]=
  {
    val listBoolean=for(i<- 1 to ds.count().toInt) yield if(Random.nextBoolean()==true) 1 else 0//Creating a list of random Boolean values
    val newDS=listBoolean.toDF(newColumnName).withColumn(joinColumnName,(monotonicallyIncreasingId()+1)) // Creating a dataframe of Boolean values generated with common column "Id"
    ds.join(newDS,joinColumnName)
  }


  kaggleHouseData = generateBooleanData(kaggleHouseData,"CloseToCommute","Id")

  def renameExistingColumns(newColumnNames:Seq[String],ds: Dataset[Row]):Dataset[Row]=
    ds.toDF(newColumnNames: _*)//Renaming


  val newKaggleColumnnames=Seq("Id","YearBuilt","YearRemodelled","LivingArea","NumberOfBedrooms","SalesPrice","GarageCarCount",
    "GarageExisting","ZipCode", "NumberOfFloors","BasementExisting","NumberOfBathrooms","FireplaceExisting","PoolExisting","SalesCondition",
    "CloseToCommute")

  kaggleHouseData=renameExistingColumns(newKaggleColumnnames,kaggleHouseData)

  kaggleHouseData=kaggleHouseData.select("Id","YearBuilt","YearRemodelled","NumberOfBedrooms","NumberOfBathrooms","NumberOfFloors",
    "LivingArea","BasementExisting", "FireplaceExisting","PoolExisting","GarageExisting","GarageCarCount","CloseToCommute","SalesCondition",
    "ZipCode", "SalesPrice")

 //kaggleHouseData.show()



  //Pre-processing the KC_house_data

  //Removing unwanted columns
  //Method to remove unwanted columns


  val kcColmumnsToRemove=Seq("date","sqft_lot","waterfront","view","grade","sqft_above","lat","long","sqft_living15",
    "sqft_lot15")
  kcHouseData = removeColumns(kcColmumnsToRemove,kcHouseData)

  //TotalBsmtSF has the basement in sq.ft. We are keeping only the data like whether house is having basement or not (0 or 1).
  import sqlContext.implicits._ // for  $""
  kcHouseData = kcHouseData.withColumn("BasementExisting", when($"sqft_basement" === "0", 0).otherwise(1))
  kcHouseData=kcHouseData.drop("sqft_basement")//Removing the original column


  //Simulating the data for CloseToCommute,FireplaceExisting,PoolExisting columns
  kcHouseData = generateBooleanData(kcHouseData,"CloseToCommute","Id")
  kcHouseData = generateBooleanData(kcHouseData,"FireplaceExisting","Id")
  kcHouseData = generateBooleanData(kcHouseData,"PoolExisting","Id")

  /*//NumberOfBathrooms has values like 1,1.25,1.5,1.75 and so on. We are trying to make 1, 1.5, 2 and so
  //If 1.25 making it as 1 and 1.75 as 2
  kcHouseData = kcHouseData.withColumn("bathrooms", when($"bathrooms".substr(1,3)===".25",$"bathrooms".substr(1,1)).otherwise())
  kaggleHouseData = kaggleHouseData.withColumn("PoolExisting", when($"PoolArea" === "0", 0).otherwise(1))
*/

  val newkcColumnnames=Seq("Id","SalesPrice","NumberOfBedrooms","NumberOfBathrooms","GarageCarCount","GarageExisting","LivingArea","NumberOfFloors","SalesCondition","YearBuilt","YearRemodelled",
    "ZipCode","BasementExisting","CloseToCommute","FireplaceExisting","PoolExisting")

  kcHouseData=renameExistingColumns(newkcColumnnames,kcHouseData)

  kcHouseData=kcHouseData.select("Id","YearBuilt","YearRemodelled","NumberOfBedrooms","NumberOfBathrooms","NumberOfFloors",
    "LivingArea","BasementExisting", "FireplaceExisting","PoolExisting","GarageExisting","GarageCarCount","CloseToCommute","SalesCondition",
    "ZipCode", "SalesPrice")

  //kcHouseData.show()

  //Merging Kaggle House Data and KC House Data
  var combinedData = kaggleHouseData.union(kcHouseData)
  //println(combinedData.count())

  //Pre-processing ZipCode data

  //Removing the unwanted columns
  val zipcodeColmumnsToRemove=Seq("Id","ZipCodeType","LocationType","Lat","Long","Xaxis","Yaxis","Zaxis",
    "WorldRegion","Country","LocationText","Location","Decommisioned","TaxReturnsFiled","EstimatedPopulation","TotalWages","Notes"
  )
  zipcodeData = removeColumns(zipcodeColmumnsToRemove,zipcodeData)

  //Merging ZipCode Data with States Data for having state's full name
  zipcodeData = zipcodeData.join(statesData,zipcodeData.col("StateAbbreviation")===statesData.col("Abbreviation"))
  zipcodeData=zipcodeData.drop("Abbreviation")
  //zipcodeData.show()


  //Merging ZipCode Data with combined Kaggle and  KC housing Data

  combinedData=combinedData.join(zipcodeData,"ZipCode")
  println(combinedData.count())

  //Id column will now be having duplicate records because Id in both Kaggle and KC Housing data are starting from 1.
  // So we are trying to remove make it continuous integers from 1 to n
  combinedData=combinedData.withColumn("NewId",functions.row_number().over(Window.orderBy("Id")))
  combinedData=combinedData.drop("Id")
  combinedData=combinedData.withColumnRenamed("NewId","Id")

  //Re-ordering the columns
  combinedData = combinedData.select("Id","YearBuilt","YearRemodelled","NumberOfBedrooms","NumberOfBathrooms","NumberOfFloors",
    "LivingArea","BasementExisting", "FireplaceExisting","PoolExisting","GarageExisting","GarageCarCount","CloseToCommute","SalesCondition",
    "ZipCode","City","StateAbbreviation","State","SalesPrice")
  combinedData=combinedData.orderBy("Id")
  //combinedData.show()


  // Sales Price is a non-nullable column
 //Replacing "NaN" and "Null" values with mean for SalesPrice column
  import org.apache.spark.sql.functions.mean
  combinedData = combinedData.na.fill(combinedData.select(mean("SalesPrice")).first()(0).asInstanceOf[Double], Seq("SalesPrice"))
  //combinedData.na.fill(combinedData.columns.zip(combinedData.select(combinedData.columns.map(mean(_)): _*).first.toSeq).toMap)



  //Saving the final data to MySQL table

  //create properties object
  val prop=new Properties()
  prop.setProperty("driver", "com.mysql.jdbc.Driver")
  prop.setProperty("user", "root")
  prop.setProperty("password", "Infy123+")

  //jdbc mysql url - destination database is named "scala_project"
  val url = "jdbc:mysql://localhost:3306/scala_project"
  //destination database table
  val table = "housing"

  //write data from spark dataframe to database
  combinedData.write.mode("Overwrite").jdbc(url, table, prop)
  //By default Spark creates and inserts data into the destination table.
  // If the table already exists, we will get a TableAlreadyExists Exception.
  // To avoid this exception, mode("append") is used;
  // It will create and insert if table is not existing; if existing it will append to existing table.
  // To avoid this exception, mode("Overwrite") is used;
  // It will create and insert if table is not existing; if existing it will overwrite the existing table.




}
