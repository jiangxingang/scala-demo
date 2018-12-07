/**
  *
  * bin/spark-submit --class "com.xxx.demo.HiveShowDB" test/demo-0.0.1-SNAPSHOT.jar
  */

package com.xxx.demo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * 显示HIVE数据库
  * @author jxg
  */
object HiveShowDB {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("TestHiveShowDB").setMaster("local")

    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    spark.sql("show databases").collect().foreach(println)

    //测试选择数据库
    spark.sql("use spark_hive")

    //测试打印数据
    val rdd = spark.sql("select * from teachers").collect()
    println(rdd.toBuffer)

    spark.stop()
  }
}