/**
  * JSON
  *
{"name": "michael"}
{"name": "andy","age": 30}
{"name": "justin","age": 19}
  *
  *  bin/spark-submit --class "com.xxx.demo.JsonReader" test/demo-0.0.1-SNAPSHOT.jar
  */

package com.xxx.demo

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util.parsing.json.JSON

/**
  * 解析JSON文件
  * @author jxg
  */
object JsonReader {
  def main(args: Array[String]) {
    val inputFile = "file:///usr/local/hadoop/test/pelope.json"
    val conf = new SparkConf().setAppName("JSONRead")
    val sc = new SparkContext(conf)
    val jsonStrs = sc.textFile(inputFile)
    val result = jsonStrs.map(s => JSON.parseFull(s))
    result.foreach({ r =>
      r match {
        case Some(map: Map[String, Any]) => println(map)
        case None => println("Parsing failed")
        case other => println("Unknown data structure: " + other)
      }
    }
    )
  }
}
