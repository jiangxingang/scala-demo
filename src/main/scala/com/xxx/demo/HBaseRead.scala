/**
  * hbase启动，停止
  * bin/start-hbase.sh
  * bin/hbase shell
  * bin/stop-hbase.sh
  *
  * bin/spark-submit --class "com.xxx.demo.HBaseRead" test/demo-0.0.1-SNAPSHOT.jar
  *
  * 不成功，怀疑版本问题。有知都请告诉我。
  */

package com.xxx.demo

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.hadoop.hbase.mapreduce

object HBaseRead {
  def main(args: Array[String]) {
    val conf = HBaseConfiguration.create()
    val sc = new SparkContext(new SparkConf())
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "student")
    val stuRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val count = stuRDD.count()
    println("Students RDD Count:" + count)
    stuRDD.cache()

    //遍历输出
    stuRDD.foreach({ case (_,result) =>
      val key = Bytes.toString(result.getRow)
      val name = Bytes.toString(result.getValue("info".getBytes,"name".getBytes))
      val gender = Bytes.toString(result.getValue("info".getBytes,"gender".getBytes))
      val age = Bytes.toString(result.getValue("info".getBytes,"age".getBytes))
      println("Row key:"+key+" Name:"+name+" Gender:"+gender+" Age:"+age)
    })
  }
}
