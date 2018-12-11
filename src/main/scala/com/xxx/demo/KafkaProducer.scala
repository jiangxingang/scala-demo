/**
  * 1、启动zookeeper
  * bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
  *
  * 2、启动kafka
  * bin/kafka-server-start.sh -daemon config/server.properties
  *
  * 3、创建topic
  * bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic dblab
  *
  * 4、查看topic
  * bin/kafka-topics.sh --list --zookeeper localhost:2181
  *
  * 下载监控
  * https://www.cnblogs.com/dadonggg/p/8242682.html
  * 开启监控
  * java -cp KafkaOffsetMonitor-assembly-0.2.0.jar com.quantifind.kafka.offsetapp.OffsetGetterWeb --zk 192.168.56.108:2181 --port 8088  --refresh 5.seconds --retain 1.days
  *
  * bin/spark-submit --class "com.xxx.demo.KafkaProducer" test/demo-0.0.1-SNAPSHOT.jar
  */

package com.xxx.demo

import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * scala kafak 发布者
  * @author jxg
  */
object KafkaProducer {

  def main(args: Array[String]): Unit = {

    val brokers = "127.0.0.1:9092" //zookeeper代理节点
    val inputTopic = "dblab" //topic

    //设置zookeeper连接属性
    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    //初始化生产者
    val producer = new KafkaProducer[String, String](props)

    val inputFile = "/usr/local/dbtaobao/dataset/small_user_log.csv"
    val in = Source.fromFile(inputFile)
    val lines = in.getLines()
    for (line <- lines) {
      val key = null
      val msg = line.split(",")(9)
      val message = new ProducerRecord[String, String](inputTopic, key, msg)
      producer.send(message)
      println(line)
      Thread.sleep(100)
    }
    producer.close()


//    //发送消息
//    while(true) {
//      val key = null
//      val value = "yang yun ni hao sha a"
//      val message = new ProducerRecord[String, String](inputTopic, key, value)
//      producer.send(message)
//      println(message)
//      Thread.sleep(1000)
//    }

// kafka 0.8
//    val inputFile = "/usr/local/dbtaobao/dataset/small_user_log.csv"
//    val conf = new SparkConf().setAppName("KafkaProducer")
//    val sc = new SparkContext(conf)
//
//    val props = new Properties();
//
//    props.put("metadata.broker.list","127.0.0.1:9092")
//    props.put("serializer.class", "kafka.serializer.StringEncoder")
//    props.put("request.required.acks", "1")
//
//    val config = new ProducerConfig(props);
//
//    val producer = new Producer[String,String](config)
//
//    val topic = "dblab"
//    val message = "abcdef"
//
//    //val data = new KeyedMessage[String,String](topic, message)
//    //producer.send(data)
//    //println("send msg acdef")
//
//    val in = Source.fromFile(inputFile)
//    val lines = in.getLines()
//    for (line <- lines) {
//      val msg = line.split(",")(9)
//      val data = new KeyedMessage[String,String](topic, msg)
//      producer.send(data)
//      println(line)
//      Thread.sleep(100)
//    }
//
//    producer.close()
  }
}