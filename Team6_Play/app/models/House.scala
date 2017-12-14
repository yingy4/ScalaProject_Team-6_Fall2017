package model

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.PipelineModel
import play.api.Play
import play.api.data.Form
import play.api.data.Forms._
import play.api.db.slick.DatabaseConfigProvider
import org.apache.spark.sql.{SQLContext, SparkSession}
import scala.concurrent.Future
import slick.driver.JdbcProfile
import slick.driver.MySQLDriver.api._

import scala.concurrent.ExecutionContext.Implicits.global

case class House(id: Long,yearBuilt: Int, yearRemodelled: Int,numberOfBedrooms:Int, numberOfBathrooms:Int,numberOfFloors:Int,
                 livingArea: Int,basementExisting:Int,fireplaceExisting :Int,poolExisting:Int,garageExisting:Int,garageCarCount:Int,
                 closeToCommute: Int,salesCondition:Int,zipCode:Int,salesPrice:String)

case class HouseFormData(yearBuilt: Int, yearRemodelled: Int,numberOfBedrooms:Int, numberOfBathrooms:Int,numberOfFloors:Int,
                         livingArea: Int,basementExisting:Int,fireplaceExisting :Int,poolExisting:Int,garageExisting:Int,garageCarCount:Int,
                         closeToCommute: Int,salesCondition:Int,zipCode:Int)

object HouseForm {

  val form = Form(
    mapping(
      "yearBuilt" -> number,
      "yearRemodelled" -> number,
      "numberOfBedrooms" -> number,
      "numberOfBathrooms" -> number,
      "numberOfFloors" -> number,
      "livingArea" -> number,
      "basementExisting" -> number,
      "fireplaceExisting" -> number,
      "poolExisting" -> number,
      "garageExisting" -> number,
      "garageCarCount" -> number,
      "closeToCommute" -> number,
      "salesCondition" -> number,
      "zipCode" -> number
    )(HouseFormData.apply)(HouseFormData.unapply)
  )
}

class HouseTableDef(tag: Tag) extends Table[House](tag, "testHouses") {

  def id = column[Long]("id")
  def yearBuilt = column[Int]("yearBuilt")
  def yearRemodelled = column[Int]("yearRemodelled")
  def numberOfBedrooms = column[Int]("numberOfBedrooms")
  def numberOfBathrooms = column[Int]("numberOfBathrooms")
  def numberOfFloors = column[Int]("numberOfFloors")
  def livingArea = column[Int]("livingArea")
  def basementExisting = column[Int]("basementExisting")
  def fireplaceExisting = column[Int]("fireplaceExisting")
  def poolExisting = column[Int]("poolExisting")
  def garageExisting = column[Int]("garageExisting")
  def garageCarCount = column[Int]("garageCarCount")
  def closeToCommute = column[Int]("closeToCommute")
  def salesCondition = column[Int]("salesCondition")
  def zipCode = column[Int]("zipCode")
  def salesPrice = column[String]("salesPrice")
  override def * =
    (id, yearBuilt, yearRemodelled, numberOfBedrooms, numberOfBathrooms,numberOfFloors,livingArea,
      basementExisting,fireplaceExisting,poolExisting,garageExisting,garageCarCount,
      closeToCommute,salesCondition,zipCode,salesPrice) <>(House.tupled, House.unapply)
}

object Houses {

  val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Play").set("spark.executor.memory","1g");
  val sparkContext = SparkContext.getOrCreate(sparkConf)
  val sparkSession = SparkSession.builder.appName("Play").config("spark.master", "local").getOrCreate
  val sqlContext = new SQLContext(sparkContext)

  val dbConfig = DatabaseConfigProvider.get[JdbcProfile](Play.current)

  val houses = TableQuery[HouseTableDef]


  //Method to add new house
  def add(house: House): Future[String] = {
    dbConfig.db.run(houses += house).map(res => "House successfully added").recover {
      case ex: Exception => ex.getCause.getMessage

    }
  }

  // Method to predict the Sales Price of the house
  def predict(id:Long):String={

    val sparkContext = SparkContext.getOrCreate(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

      val newModel = PipelineModel.load("./PredictionModels/RandomForestModel")
      val testRaw = sqlContext.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/scala_project").
        option("driver", "com.mysql.jdbc.Driver").
        option("dbtable", "testHouses").
        option("user", "root").
        option("password", "Infy123+").load()
      testRaw.registerTempTable("Test_Data")
      val newTestData=  sqlContext.sql("""SELECT int(YearBuilt) YearBuilt,int(YearRemodelled) YearRemodelled,
                  int(NumberOfBedrooms) NumberOfBedrooms,double(NumberOfBathrooms) NumberOfBathrooms,
                  double(NumberOfFloors) NumberOfFloors,int(LivingArea) LivingArea,int(BasementExisting) BasementExisting,
                  int(FireplaceExisting) FireplaceExisting,int(PoolExisting) PoolExisting,int(GarageExisting) GarageExisting,
                  int(GarageCarCount) GarageCarCount,int(CloseToCommute) CloseToCommute,int(SalesCondition) SalesCondition,int(ZipCode) ZipCode
                  FROM Test_Data""").na.drop()
      val predictedSalesPrice = newModel.transform(newTestData).select("prediction").first().getDouble(0)
      String.valueOf(predictedSalesPrice)
  }

  def delete(id: Long): Future[Int] = {
    dbConfig.db.run(houses.filter(_.id === id).delete)
  }

  def listAll: Future[Seq[House]] = {
    dbConfig.db.run(houses.result)
  }


  sparkContext.stop()

}
