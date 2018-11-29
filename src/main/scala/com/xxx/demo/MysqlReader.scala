/**
  *
  * 下载MySQL的JDBC驱动程序，比如mysql-connector-java-5.1.40.tar.gz
  * 把该驱动程序拷贝到spark的安装目录” /usr/local/spark/jars”下
  *
  * ./bin/spark-shell --jars /usr/local/spark/jars/mysql-connector-java-5.1.47.jar --driver-class-path /usr/local/spark/jars/mysql-connector-java-5.1.47.jar
  *
  * bin/spark-submit --class "com.xxx.demo.MysqlReader" test/demo-0.0.1-SNAPSHOT.jar
  */

package com.xxx.demo

import java.sql.DriverManager
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object MysqlReader {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JdbcRDDDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val connection = () => {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://192.168.251.45:3306/hato-billing","root","123456")
    }
    //这个地方没有读取数据(数据库表也用的是table_name)
    val jdbcRDD = new JdbcRDD(
      sc,
      connection,
      "SELECT * FROM t_order where id >= ? AND id <= ?",
      //这里表示从取数据库中的第1、2、3、4条数据，然后分两个区
      1, 4, 2,
      r => {
        val id = r.getInt(1)
        val code = r.getString(2)
        (id, code)
      }
    )
    //这里相当于是action获取到数据
    val jrdd = jdbcRDD.collect()
    println(jrdd.toBuffer)
    sc.stop()
  }
}
