/**
  * 第一：HDFS管理
  * 进到hadoop:
  * 启动
  * ./sbin/start-dfs.sh
  * 停止
  *./sbin/stop-dfs.sh
  *
  * ./bin/hdfs dfs -mkdir -p /user/hadoop
  * ./bin/hdfs dfs -mkdir input
  * ./bin/hdfs dfs -put ./etc/hadoop/ *.xml input
  * ./bin/hdfs dfs -ls input
  * ./bin/hdfs dfs -cat output/ *
  * ./bin/hdfs dfs -get output ./output     # 将 HDFS 上的 output 文件夹拷贝到本机
  * ./bin/hdfs dfs -rm -r output    # 删除 output 文件夹
  *
  * 第二：运行命令
  * bin/spark-submit --class "com.xxx.demo.Break" test/demo-0.0.1-SNAPSHOT.jar
  * 上面命令执行后会输出太多信息，可以不使用上面命令，而使用下面命令查看想要的结果
  *
    18/11/27 14:07:49 DEBUG scheduler.DAGScheduler: After removal of stage 4, remaining stages = 1
    18/11/27 14:07:49 DEBUG scheduler.DAGScheduler: After removal of stage 3, remaining stages = 0
    18/11/27 14:07:49 INFO scheduler.DAGScheduler: Job 2 finished: take at HdfsTopN.scala:17, took 0.318734 s
    1	7890
    2	788
    3	600
    4	489
    5	290
    18/11/27 14:07:49 INFO spark.SparkContext: Invoking stop() from shutdown hook
    18/11/27 14:07:49 DEBUG component.AbstractLifeCycle: stopping org.spark_project.jetty.server.Server@53967a9b
  *
  */

package com.xxx.demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 从HDFS读取数据进行排序
  * @author jxg
  */
object HdfsTopN {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HdfsTopN").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("DEBUG")

    val lines = sc.textFile("hdfs://localhost:9000/user/hadoop/input", 2)
    var num = 0;
    val result = lines.filter(line => (line.trim().length > 0) && (line.split(",").length == 4))
      .map(_.split(",")(2))
      .map(x => (x.toInt, ""))
      .sortByKey(false)
      .map(x => x._1).take(5)
      .foreach(x => {
        num = num + 1
        println(num + "\t" + x)
      })
  }
}
