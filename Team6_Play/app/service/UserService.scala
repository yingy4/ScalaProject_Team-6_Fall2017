package service

import model.{User, Users}

import scala.concurrent.Future

object UserService {

  def addUser(user: User): Future[String] = {
    Users.add(user)
  }

  def deleteUser(idUser: Long): Future[Int] = {
    Users.delete(idUser)
  }

  def getUser(idUser: Long): Future[Option[User]] = {
    Users.get(idUser)
  }

  def listAllUsers: Future[Seq[User]] = {
    Users.listAll
  }
}