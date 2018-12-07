/**
  * SVM理论：https://www.cnblogs.com/jerrylead/archive/2011/03/13/1982639.html
  *
  * 来源：http://dblab.xmu.edu.cn/blog/1268/
  * 数据：https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data
  *      http://dblab.xmu.edu.cn/blog/1381-2/
  *
  * bin/spark-submit --class "com.xxx.demo.SvmTest" test/demo-0.0.1-SNAPSHOT.jar
 */

package com.xxx.demo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

object SvmTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SvmTest").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("INFO")

    //读取本机文件，简单测试，没有使用hdfs
    val data = sc.textFile("file:///opt/iris.txt")

    val parsedData = data.map { line =>
           val parts = line.split(',')
           LabeledPoint(if(parts(4)=="Iris-setosa") 0.toDouble else if (parts(4) =="Iris-versicolor") 1.toDouble else 2.toDouble,
             Vectors.dense(parts(0).toDouble, parts(1).toDouble,parts(2).toDouble,parts(3).toDouble))
    }

    val splits = parsedData.filter { point => point.label != 2 }.randomSplit(Array(0.5, 0.5), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    val numIterations = 1000
    val model = SVMWithSGD.train(training, numIterations)

    model.clearThreshold()

    println("training count = " + training.count())
    println("test count = " + test.count())

    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    scoreAndLabels.foreach(println)

    model.setThreshold(0.0)
    scoreAndLabels.foreach(println)

    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()
    println("Area under ROC = " + auROC)
  }
}
