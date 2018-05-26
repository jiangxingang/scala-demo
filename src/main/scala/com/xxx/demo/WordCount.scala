package com.xxx.demo

import java.io.File

import scala.io.Source
;

object WordCount {
  def main(args: Array[String]): Unit = {

    val dirFile = new File("/Users/DMMac/Downloads/abc")
    val files = dirFile.listFiles()
    var results = scala.collection.mutable.Map.empty[String, Int]

    for(file <- files){
      val data = Source.fromFile(file)
      val strs = data.getLines().flatMap{s => { s.split(" ")}}
      strs foreach { word =>
        if (results.contains(word))
          results(word) += 1
        else
          results(word) = 1
      }
    }

    results foreach{case(k, v) => println(s"$k : $v")}
  }
}
