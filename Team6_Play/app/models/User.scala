package model

import play.api.Play
import play.api.data.Form
import play.api.data.Forms._
import play.api.db.slick.DatabaseConfigProvider
import scala.concurrent.Future
import slick.driver.JdbcProfile
import slick.driver.MySQLDriver.api._
import scala.concurrent.ExecutionContext.Implicits.global

case class User(idUser: Long, firstName: String, lastName: String, mobile: Long, email: String,sellerBuyer: String )

case class UserFormData(firstName: String, lastName: String, mobile: Long, email: String,sellerBuyer: String )

object UserForm {

  val form1 = Form(
    mapping(
      "firstName" -> nonEmptyText,
      "lastName" -> nonEmptyText,
      "mobile" -> longNumber,
      "email" -> email,
      "sellerBuyer" -> nonEmptyText
    )(UserFormData.apply)(UserFormData.unapply)
  )
}

class UserTableDef(tag: Tag) extends Table[User](tag, "Users") {

  def idUser = column[Long]("idUser", O.PrimaryKey,O.AutoInc)
  def firstName = column[String]("first_name")
  def lastName = column[String]("last_name")
  def mobile = column[Long]("mobile")
  def email = column[String]("email")
  def sellerBuyer = column[String]("sellerBuyer")

  override def * =
    (idUser, firstName, lastName, mobile, email,sellerBuyer) <>(User.tupled, User.unapply)
}

object Users {

  val dbConfig = DatabaseConfigProvider.get[JdbcProfile](Play.current)

  val users = TableQuery[UserTableDef]

  def add(user: User): Future[String] = {
    dbConfig.db.run(users += user).map(res => "User successfully added").recover {
      case ex: Exception => ex.getCause.getMessage
    }
  }

  def delete(idUser: Long): Future[Int] = {
    dbConfig.db.run(users.filter(_.idUser === idUser).delete)
  }

  def get(idUser: Long): Future[Option[User]] = {
    dbConfig.db.run(users.filter(_.idUser === idUser).result.headOption)
  }

  def listAll: Future[Seq[User]] = {
    dbConfig.db.run(users.result)
  }

}