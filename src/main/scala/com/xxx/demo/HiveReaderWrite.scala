/**
  *
  * 步骤：
  * 1、在hive中创建两个表
  * create table  teacher_basic
  * create table  teacher_info
  * 2、将数据加载进这两表中
  * load data local inpath  '/opt/teacher_basic.txt' into table teacher_basic
  * load data local inpath  '/opt/teacher_info.txt' into table teacher_info
  * 3、进行hiveql的join操作
  * select id, name, age, height, married, children from  teacher_basic
  * tb left join teacher_info ti on tb.name = ti.name
  * 4、将计算之后的(步骤3)数据保存到一张表teachers
  *
  * bin/spark-submit --class "com.xxx.demo.HiveReaderWrite" test/demo-0.0.1-SNAPSHOT.jar
  */

package com.xxx.demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf}

/**
  * 显示HIVE数据库
  *
  * @author jxg
  */
object HiveReaderWrite {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("TestHiveShowDB").setMaster("local")

    //如果不加，创建的库在spark warehouse下
    val warehouse = "/user/hive/warehouse"
    conf.set("spark.sql.warehouse.dir", warehouse)

    val hiveSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    //step-1、在Hive中创建两张表
    hiveSession.sql("create database if not exists spark_hive")
    //name,age,married,children
    hiveSession.sql("create table if not exists spark_hive.teacher_basic(name string, age int, married boolean, children int) row format delimited fields terminated by ','")
    hiveSession.sql("create table if not exists spark_hive.teacher_info(name string, height int) row format delimited fields terminated by ','")

    //step-2: 加载数据到这两张表中
    hiveSession.sql("load data local inpath '/opt/teacher_basic.txt' overwrite into table spark_hive.teacher_basic")
    hiveSession.sql("load data local inpath '/opt/teacher_info.txt' overwrite into table spark_hive.teacher_info")

    //step-3:进行多表关联计算
    val joinedDF = hiveSession.sql("select tb.name, tb.age, tb.married, ti.height, tb.children from spark_hive.teacher_basic tb left join spark_hive.teacher_info ti on tb.name = ti.name")
    //step-4:数据落地
    joinedDF.write.saveAsTable("spark_hive.teachers")
    hiveSession.stop()
  }
}