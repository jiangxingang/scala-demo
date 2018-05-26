package com.xxx.demo

import scala.io.Source
;

/**
  * 文件读取
  */
object ReadFile {
  def main(args: Array[String]): Unit = {
    val in = Source.fromFile("/Users/DMMac/Downloads/a.txt")
    val lines = in.getLines()
    for (line <- lines if line.startsWith("1")) {

      //if (line.startsWith("1")){
      //  println("a");
      //}

      println(line)
    }


    //无效
    //for(line2 <- lines if line2.startsWith("1")) println("b")

    // for 推导式
    val result = for (i <- Array(1,2,3,4,5) if i % 2 == 0) yield {println(i); i}

    for(j <- result) println(j)
  }
}
