package com.fz.classification

import com.fz.util.Utils
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.optimization.L1Updater

/**
 * 逻辑回归封装算法
 *  Labels used in SVM should be {0, 1}
 * 输入参数：
 * testOrNot : 是否是测试，正常情况设置为false
 * input：输出数据；
 * minPartitions: 输入数据最小partition个数
 * output：输出路径
 * targetIndex：目标列所在下标，从1开始
 * splitter：数据分隔符；
 * numIteration：循环次数，默认100
 * stepSize : 步进，默认1.0
 * regParam: 正则参数，默认0.01
 * miniBatchFraction: 批处理粒度，默认1.0 Fraction of data to be used per iteration
 * regMethod : 正则化函数，"L1" 或 ”L2“
 * Created by fanzhe on 2016/12/30.
 */
object SVM {

   def main (args: Array[String]) {
    if(args.length != 11){
      println("Usage: com.fz.classification.SVM testOrNot input minPartitions output targetIndex " +
        "splitter numIteration stepSize regParam miniBatchFraction regMethod")
      System.exit(-1)
    }
     val testOrNot = args(0).toBoolean // 是否是测试，sparkContext获取方式不一样, true 为test
     val input = args(1)
     val minPartitions = args(2).toInt
     val output = args(3)
     val targetIndex = args(4).toInt // 从1开始，不是从0开始要注意
     val splitter = args(5)
     val numIteration = args(6).toInt //
     val stepSize = args(7).toDouble
     val regParam = args(8).toDouble
     val miniBatchFraction = args(9).toDouble
     val regMethod = args(10)
     // 删除输出，不在Scala算法里面删除，而在Java代码里面删除
     //     Utils.deleteOutput(output)

     val sc =  Utils.getSparkContext(testOrNot,"SVM create Model")

     // construct data
     // Load and parse the data
     val training = Utils.getLabeledPointData(sc,input,minPartitions,splitter,targetIndex).cache()

     // Run training algorithm to build the model
     val model = regMethod match {
       case "L1" => val svmAlg = new SVMWithSGD()
         svmAlg.optimizer.
           setNumIterations(numIteration).
           setRegParam(regParam).
           setMiniBatchFraction(miniBatchFraction).
           setUpdater(new L1Updater)
         svmAlg.run(training)
       case "L2" =>  SVMWithSGD.train(training, numIteration,stepSize,regParam)
       case _ => throw new RuntimeException("no regMethod specific")
     }
     // save model

     model.save(sc,output)

     sc.stop()
  }
}
