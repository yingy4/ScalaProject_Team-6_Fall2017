package service

import model.{House, Houses}
import scala.concurrent.Future

object HouseService {

  def addHouse(house: House): Future[String] = {
    Houses.add(house)
  }
  def predict(id: Long):String = {
    Houses.predict(id)
  }

  def deleteHouse(id: Long): Future[Int] = {
    Houses.delete(id)
  }

  def listAllHouses: Future[Seq[House]] = {
    Houses.listAll
  }

}
