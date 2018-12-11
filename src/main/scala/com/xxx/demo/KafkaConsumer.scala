/**
  * bin/spark-submit --class "com.xxx.demo.KafkaConsumer" test/demo-0.0.1-SNAPSHOT.jar
  *
  * kafka版本问题，需要修改成这个命令
  * bin/spark-submit --driver-class-path /usr/local/spark/jars/*:/usr/local/spark/jars/kafka/* --class "com.xxx.demo.KafkaConsumer" test/demo-0.0.1-SNAPSHOT.jar
  *
  * 未知原因：有知道的请告知
  Exception in thread "pool-22-thread-25" java.lang.NullPointerException
	at org.apache.spark.streaming.CheckpointWriter$CheckpointWriteHandler.run(Checkpoint.scala:233)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)
  *
  */*/*/
package com.xxx.demo

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import scala.collection.JavaConversions._

import scala.collection.mutable

object KafkaConsumer {

  // ZK client
  val client = {
    val client = CuratorFrameworkFactory
      .builder
      .connectString("127.0.0.1:2181")
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .namespace("mykafka")
      .build()
    client.start()
    client
  }

  // offset 路径起始位置
  val Globe_kafkaOffsetPath = "/kafka/offsets"

  // 路径确认函数  确认ZK中路径存在，不存在则创建该路径
  def ensureZKPathExists(path: String)={

    if (client.checkExists().forPath(path) == null) {
      client.create().creatingParentsIfNeeded().forPath(path)
    }

  }


  // 保存 新的 offset
  def storeOffsets(offsetRange: Array[OffsetRange], groupName:String) = {

    for (o <- offsetRange){
      val zkPath = s"${Globe_kafkaOffsetPath}/${groupName}/${o.topic}/${o.partition}"

      // 向对应分区第一次写入或者更新Offset 信息
      println("---Offset写入ZK------\nTopic：" + o.topic +", Partition:" + o.partition + ", Offset:" + o.untilOffset)
      client.setData().forPath(zkPath, o.untilOffset.toString.getBytes())
    }
  }

  def getFromOffset(topic: Array[String], groupName:String):(Map[TopicPartition, Long], Int) = {

    // Kafka 0.8和0.10的版本差别，0.10 为 TopicPartition   0.8 TopicAndPartition
    var fromOffset: Map[TopicPartition, Long] = Map()

    val topic1 = topic(0).toString

    // 读取ZK中保存的Offset，作为Dstrem的起始位置。如果没有则创建该路径，并从 0 开始Dstream
    val zkTopicPath = s"${Globe_kafkaOffsetPath}/${groupName}/${topic1}"

    // 检查路径是否存在
    ensureZKPathExists(zkTopicPath)

    // 获取topic的子节点，即 分区
    val childrens = client.getChildren().forPath(zkTopicPath)

    // 遍历分区
    val offSets: mutable.Buffer[(TopicPartition, Long)] = for {
      p <- childrens
    }
      yield {

        // 遍历读取子节点中的数据：即 offset
        val offsetData = client.getData().forPath(s"$zkTopicPath/$p")
        // 将offset转为Long
        val offSet = java.lang.Long.valueOf(new String(offsetData)).toLong
        // 返回  (TopicPartition, Long)
        (new TopicPartition(topic1, Integer.parseInt(p)), offSet)
      }
    println(offSets.toMap)

    if(offSets.isEmpty){
      (offSets.toMap, 0)
    } else {
      (offSets.toMap, 1)
    }


  }

  //    if (client.checkExists().forPath(zkTopicPath) == null){
  //
  //      (null, 0)
  //    }
  //    else {
  //      val data = client.getData.forPath(zkTopicPath)
  //      println("----------offset info")
  //      println(data)
  //      println(data(0))
  //      println(data(1))
  //      val offSets = Map(new TopicPartition(topic1, 0) -> 7332.toLong)
  //      println(offSets)
  //      (offSets, 1)
  //    }
  //
  //  }

  def createMyZookeeperDirectKafkaStream(ssc:StreamingContext, kafkaParams:Map[String, Object], topic:Array[String],
                                         groupName:String ):InputDStream[ConsumerRecord[String, String]] = {

    // get offset  flag = 1  表示基于已有的offset计算  flag = 表示从头开始(最早或者最新，根据Kafka配置)
    val (fromOffsets, flag) = getFromOffset(topic, groupName)
    var kafkaStream:InputDStream[ConsumerRecord[String, String]] = null
    if (flag == 1){
      // 加上消息头
      //val messageHandler = (mmd:MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
      println(fromOffsets)
      kafkaStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe(topic, kafkaParams, fromOffsets))
      println(fromOffsets)
      println("中断后 Streaming 成功！")

    } else {
      kafkaStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe(topic, kafkaParams))

      println("首次 Streaming 成功！")

    }
    kafkaStream
  }


  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "/usr/local/hadoop")

    val conf = new SparkConf().setAppName("KafkaConsumer")
    conf.setMaster("local[2]")
    conf.set("fs.default.name", "hdfs://127.0.0.1:9000");

    @transient
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("/tmp")

    //创建topic
    var topic = Array("dblab");
    //指定zookeeper
    //创建消费者组
    var group = "dblab-consumer-group"
    //消费者配置
    val kafkaParam = Map(
      "bootstrap.servers" -> "127.0.0.1:9092", //用于初始化链接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      "group.id" -> group,
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      "auto.offset.reset" -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)
    );

    //创建DStream，返回接收到的输入数据
    //var stream=KafkaUtils.createDirectStream[String,String](ssc, PreferConsistent, Subscribe[String,String](topic,kafkaParam))
    //每一个stream都是一个ConsumerRecord
    //stream.map(s =>(s.key(),s.value())).print();

    //stream.checkpoint(Seconds(5))

    //    val wordCounts = stream.map(s => (s.value(), 1L))
    //      .reduceByKeyAndWindow(_+_, _-_, Seconds(1), Seconds(1), 1).foreachRDD(rdd => {
    //      if(!rdd.isEmpty()){
    //        rdd.foreachPartition(partition=>{
    //          partition.foreach(pair=>{
    //            if (!partition.isEmpty){
    //              println(pair._1 + "----------" + pair._2)
    //            }
    //          })
    //        })
    //      }
    //    })


    val stream = createMyZookeeperDirectKafkaStream(ssc, kafkaParam, topic, group)
    stream.map(s => (s.key(), s.value())).print()

    val wordCounts = stream.map(s => (s.value(), 1L))
      .reduceByKeyAndWindow(_+_, _-_, Seconds(1), Seconds(1), 1).foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.foreachPartition(partition => {
          partition.foreach(pair => {
            if (!partition.isEmpty) {
              println(pair._1 + "----------" + pair._2)
            }
          })

          storeOffsets(rdd.asInstanceOf[HasOffsetRanges].offsetRanges, group)
        })
      }
    })

    ssc.start();
    ssc.awaitTermination();
  }
}