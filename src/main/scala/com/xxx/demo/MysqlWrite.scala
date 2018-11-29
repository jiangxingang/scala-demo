/**
  *
  * 下载MySQL的JDBC驱动程序，比如mysql-connector-java-5.1.40.tar.gz
  * 把该驱动程序拷贝到spark的安装目录” /usr/local/spark/jars”下
  *
  * ./bin/spark-shell --jars /usr/local/spark/jars/mysql-connector-java-5.1.47.jar --driver-class-path /usr/local/spark/jars/mysql-connector-java-5.1.47.jar
  *
  * bin/spark-submit --class "com.xxx.demo.MysqlWrite" test/demo-0.0.1-SNAPSHOT.jar
  */

package com.xxx.demo

import java.util.Properties

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object MysqlWrite {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MySQL-Demo").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //通过并行化创建RDD
    val personRDD = sc.parallelize(Array("14 tom remark1", "15 jerry remark2", "16 kitty remark3")).map(_.split(" "))
    //通过StrutType直接指定每个字段的schema
    val schema = StructType(
      List(
        StructField("id",IntegerType,true),
        StructField("name",StringType,true),
        StructField("remark",StringType,true)
      )
    )
    //将RDD映射到rowRDD
    val rowRDD = personRDD.map(p => Row(p(0).toInt, p(1).trim, p(2).trim))
    //将schema信息应用到rowRDD上
    val personDataFrame = sqlContext.createDataFrame(rowRDD,schema)
    //创建Properties存储数据库相关属性
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "123456")
    //将数据追加到数据库
    personDataFrame.write.mode("append").jdbc("jdbc:mysql://192.168.251.45:3306/hato-billing", "t_test", prop)
    //停止SparkContext
    sc.stop()
  }
}
