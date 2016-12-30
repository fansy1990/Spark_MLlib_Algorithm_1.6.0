package com.fz.classification

import com.fz.util.Utils
import org.apache.spark.mllib.classification.{LogisticRegressionWithSGD, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 逻辑回归封装算法
 * Labels used in Logistic Regression should be {0, 1, ..., k - 1} for k classes multi-label classification problem
 * 输入参数：
 * testOrNot : 是否是测试，正常情况设置为false
 * input：输出数据；
 * minPartitions : 输入数据最小partition个数
 * output：输出路径
 * targetIndex：目标列所在下标，从1开始
 * splitter：数据分隔符；
 * method：使用逻辑回归算法："SGD" or "LBFGS"
 * hasIntercept : 是否具有截距
 * numClasses: 目标列类别个数；
 * Created by fanzhe on 2016/12/19.
 */
object LogisticRegression {

   def main (args: Array[String]) {
    if(args.length != 9){
      println("Usage: com.fz.classification.LogisticRegression testOrNot input minPartitions output targetIndex " +
        "splitter method hasIntercept numClasses")
      System.exit(-1)
    }
     val testOrNot = args(0).toBoolean // 是否是测试，sparkContext获取方式不一样, true 为test
     val input = args(1)
     val minPartitions = args(2).toInt
     val output = args(3)
     val targetIndex = args(4).toInt // 从1开始，不是从0开始要注意
     val splitter = args(5)
     val method = args(6) //should be "SGD" or "LBFGS"
     val hasIntercept = args(7).toBoolean
     val numClasses = args(8).toInt

     // 删除输出
     Utils.deleteOutput(output)

     val sc =  Utils.getSparkContext(testOrNot,"Logistic Create Model")

     // construct data
     // Load and parse the data
     val training = Utils.getLabeledPointData(sc,input,minPartitions,splitter,targetIndex).cache()

     // Run training algorithm to build the model
     val model = method match {
       case "SGD" => new LogisticRegressionWithSGD()
         .setIntercept(hasIntercept)
         .run(training)
       case "LBFGS" => new LogisticRegressionWithLBFGS().setNumClasses(numClasses)
         .setIntercept(hasIntercept)
         .run(training)
       case _ => throw new RuntimeException("no method")
     }
     // save model

     model.save(sc,output)

     sc.stop()
  }
}
