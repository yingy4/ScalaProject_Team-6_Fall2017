package edu.neu.coe.scala.DatabaseConnection

/**
  * Created by Team6 on 11/15/17.
  * The class is used to establish connection with MySQL Database.
  * Please create the database by name 'scala_project' before running.
  */

class DBConnection {
  //create the database in MySQL by name 'scala_project' before executing the code
  val url="jdbc:mysql://localhost:3306/scala_project"
  val driver="com.mysql.jdbc.Driver"
  val dbTable="housing"
  val user="root"
  val password="Infy123+"
}
