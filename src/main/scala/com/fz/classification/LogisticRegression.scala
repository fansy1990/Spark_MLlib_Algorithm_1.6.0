package com.fz.classification

import org.apache.spark.mllib.classification.{LogisticRegressionWithSGD, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 逻辑回归封装算法
 * Created by fanzhe on 2016/12/19.
 */
object LogisticRegression {

   def main (args: Array[String]) {
    if(args.length != 8){
      println("Usage: com.fz.classification.LogisticRegression testOrNot input output targetIndex " +
        "splitter method hasIntercept numClasses")
      System.exit(-1)
    }
     val testOrNot = args(0).toBoolean // 是否是测试，sparkContext获取方式不一样, true 为test
     val input = args(1)
     val output = args(2)
     val targetIndex = args(3).toInt // 从1开始，不是从0开始要注意
     val splitter = args(4)
     val method = args(5) //should be "SGD" or "LBFGS"
     val hasIntercept = args(6).toBoolean
     val numClasses = args(7).toInt

     val sc =  new SparkContext( (if(testOrNot) new SparkConf().setMaster("local[2]")
     else new SparkConf()) setAppName("Logistic Create Model"))

     // construct data
     // Load and parse the data
     val data = sc.textFile(input)
     val training = data.map { line =>
       val parts = line.split(splitter)
       LabeledPoint(parts(targetIndex-1).toDouble,
         Vectors.dense( Array.concat(parts.take(targetIndex-1), parts.drop(targetIndex)).map(_.toDouble)))
     }.cache()

     // Run training algorithm to build the model
     val model = method match {
       case "SGD" => new LogisticRegressionWithSGD()
         .setIntercept(hasIntercept)
         .run(training)
       case "LBFGS" => new LogisticRegressionWithLBFGS().setNumClasses(10)
         .setIntercept(hasIntercept)
         .run(training)
       case _ => throw new RuntimeException("no method")
     }
     // save model

     model.save(sc,output)

     sc.stop()
  }
}
