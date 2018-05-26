package com.xxx.demo

import scala.util.control.Breaks._

object Break {

  def main(args: Array[String]): Unit = {

    val array = Array(1,3,10,5,4)
    breakable{
      for ( i <- array){
        if (i> 5) break
        println(i)
      }
    }
  }
}
