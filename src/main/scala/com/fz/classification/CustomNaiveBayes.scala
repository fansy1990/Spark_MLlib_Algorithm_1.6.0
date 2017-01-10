package com.fz.classification

import com.fz.util.Utils
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.optimization.L1Updater

/**
 * 朴素贝叶斯封装算法
 * Labels used in Logistic Regression should be {0, 1, ..., k - 1} for k classes multi-label classification problem
 * 输入参数：
 * testOrNot : 是否是测试，正常情况设置为false
 * input：输出数据；
 * minPartitions : 输入数据最小partition个数
 * output：输出路径
 * targetIndex：目标列所在下标，从1开始
 * splitter：数据分隔符；
 * modelType：朴素贝叶斯类型（多项式和伯努利）："multinomial" or "bernoulli"
 * lambda : 平滑度，doule类型，通常为1.0
 *
 * Created by cuihuan on 2017/1/7.
 */
object CustomNaiveBayes {
  def main(args: Array[String]) {
    if (args.length != 8) {
      println("Usage: com.fz.classification.NaiveBayes testOrNot input minPartitions output targetIndex " +
        "splitter modelType lambda")
      System.exit(-1)
    }

    val testOrNot = args(0).toBoolean // 是否是测试，sparkContext获取方式不一样, true 为test
    val input = args(1)
    val minPartitions = args(2).toInt
    val output = args(3)
    val targetIndex = args(4).toInt // 从1开始，不是从0开始要注意
    val splitter = args(5)
    val modelType = args(6) //
    val lambda = args(7).toDouble
    // 删除输出，不在Scala算法里面删除，而在Java代码里面删除
    //     Utils.deleteOutput(output)

    val sc =  Utils.getSparkContext(testOrNot,"NaiveBayes create Model")

    // construct data
    // Load and parse the data
    val training = Utils.getLabeledPointData(sc,input,minPartitions,splitter,targetIndex).cache()
    val modelTypes =Array("multinomial","bernoulli").toList//朴素贝叶斯的类型{"multinomial","bernoulli"}
    //     Run training algorithm to build the model
    val model = modelTypes.contains(modelType) match {
      case true => NaiveBayes.train(training,lambda,modelType)
      case false =>throw new RuntimeException("the model '"+modelType+"' of NaiveBayes is undefined.Only the \"multinomial\"or \"bernoulli\" is  available ")
    }
    model.save(sc,output)
    sc.stop()
  }
}
