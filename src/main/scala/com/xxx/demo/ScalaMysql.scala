package com.xxx.demo

import java.sql.DriverManager

object ScalaMysql {

  def main(args: Array[String]) {
    // create database connection
    val dbc = "jdbc:mysql://localhost:3306/test?user=root&password=123456"
    classOf[com.mysql.jdbc.Driver]
    val conn = DriverManager.getConnection(dbc)


    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/test"
    val username = "root"
    val password = "123456"

    // do database insert
    try {
      Class.forName(driver)
      val connection = DriverManager.getConnection(url, username, password)


      val prep = conn.prepareStatement("INSERT INTO blog(title, body) VALUES (?, ?) ")
      prep.setString(1, "Nothing great was ever achieved without enthusiasm.")
      prep.setString(2, "Ralph Waldo Emerson")
      prep.executeUpdate
    }
    finally {
      conn.close
    }
  }
}