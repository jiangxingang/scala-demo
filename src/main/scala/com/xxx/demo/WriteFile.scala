package com.xxx.demo

import java.io.PrintWriter
;

/**
  * 文件写操作
  */
object WriteFile {

  def main(args: Array[String]): Unit = {
    val out = new PrintWriter("/Users/DMMac/Downloads/a.txt")
    for(i <- 1 to 100){
      out.println(i)
    }

    out.close()
  }

}
