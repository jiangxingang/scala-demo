package com.xxx.demo

import java.util.Properties

object PropertiesUtils {
  def main(args: Array[String]): Unit = {

    loadProperties("aaa")
  }

  def loadProperties(key: String): Unit = {
    val properties = new Properties()
    val in = PropertiesUtils.getClass.getClassLoader.getResourceAsStream("config_scala.properties")
    properties.load(in);
    println(properties.getProperty(key)) //读取键为ddd的数据的值
  }
}
