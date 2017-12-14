package controllers

import model.{House, HouseForm, User, UserForm}
import model.UserForm
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{SQLContext, SparkSession}
import play.api.mvc._

import scala.concurrent.Future
import service.{HouseService, UserService}
import service.UserService

import scala.concurrent.ExecutionContext.Implicits.global

class ApplicationController extends Controller {

  def index() = Action.async { implicit request =>
    HouseService.listAllHouses map { houses =>
      Ok(views.html.index(HouseForm.form, houses))
    }
  }


  def addHouse() = Action.async { implicit request =>
    HouseForm.form.bindFromRequest.fold(
      // if any error in submitted data
      errorForm => Future.successful(Ok(views.html.index(errorForm, Seq.empty[House]))),
      data => {
        HouseService.deleteHouse(2)
        val newHouse = House(1,data.yearBuilt,data.yearRemodelled,data.numberOfBedrooms,
          data.numberOfBathrooms,data.numberOfFloors,data.livingArea,data.basementExisting,
          data.fireplaceExisting,data.poolExisting,data.garageExisting,data.garageCarCount,
          data.closeToCommute,data.salesCondition,data.zipCode,"0")
        HouseService.addHouse(newHouse)
        val newHouse2 = House(2,data.yearBuilt,data.yearRemodelled,data.numberOfBedrooms,
          data.numberOfBathrooms,data.numberOfFloors,data.livingArea,data.basementExisting,
          data.fireplaceExisting,data.poolExisting,data.garageExisting,data.garageCarCount,
          data.closeToCommute,data.salesCondition,data.zipCode,HouseService.predict(1.asInstanceOf[Long]).toString)
        HouseService.deleteHouse(1)
       HouseService.addHouse(newHouse2).map(res =>
          Redirect(routes.ApplicationController.index())
        )
      })
  }

  def deleteHouse(id: Long) = Action.async { implicit request =>
    HouseService.deleteHouse(id) map { res =>
      Redirect(routes.ApplicationController.index())
    }
  }

  def user = Action.async { implicit request =>
    UserService.listAllUsers map { users =>
      Ok(views.html.user(UserForm.form1, users))
    }
  }
  def addUser() = Action.async { implicit request =>
    UserForm.form1.bindFromRequest.fold(
      // if any error in submitted data
      errorForm => Future.successful(Ok(views.html.user(errorForm, Seq.empty[User]))),
      data => {
        val newUser = User(1,data.firstName,data.lastName,data.mobile,
          data.email,data.sellerBuyer)
        UserService.addUser(newUser).map(res =>
          Redirect(routes.ApplicationController.user())
        )
      })
  }

  def deleteUser(idUser: Long) = Action.async { implicit request =>
    UserService.deleteUser(idUser) map { res =>
      Redirect(routes.ApplicationController.agent())
    }
  }

  def agent = Action.async { implicit request =>
    UserService.listAllUsers map { users =>
      Ok(views.html.agent(users))
    }

  }



}

