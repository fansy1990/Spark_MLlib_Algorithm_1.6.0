package com.fz.classification

import com.fz.util.Utils
import org.apache.spark.mllib.tree.DecisionTree

/**
 *决策树-分类封装算法
 *  可用于二元分类、多元分类 Labels should take values {0, 1, ..., numClasses-1}
 *  回归：labels are real numbers(标签是实数)
 * 输入参数：
 * testOrNot : 是否是测试，正常情况设置为false
 * input：输出数据；
 * minPartitions: 输入数据最小partition个数
 * output：输出路径
 * targetIndex：目标列所在下标，从1开始
 * splitter：数据分隔符；
 * impurity：不纯度度函数，分类的有：信息熵(entropy)和基尼指数(gini)
 * maxDepth：树的最大深度，
 * algo:分类还是回归(classification,regression)
 * maxBins：设定分裂数据集  suggestion 32
 * numClasses: 类别的个数
 * Created by cuihuan on 2017/1/16.
 */
object CustomDecisionTree {
  def main (args: Array[String]) {
    if(args.length != 10 && args.length != 11){
      println("Usage: com.fz.classification.DecisionTreeClassification testOrNot input minPartitions output targetIndex " +
        "splitter impurity maxDepth algo maxBins numClasses<numClass is only for classifictation> ")
      System.exit(-1)
    }
    val testOrNot = args(0).toBoolean // 是否是测试，sparkContext获取方式不一样, true 为test
    val input = args(1)
    val minPartitions = args(2).toInt
    val output = args(3)
    val targetIndex = args(4).toInt // 从1开始，不是从0开始要注意
    val splitter = args(5)
    val impurity = args(6) //
    val maxDepth = args(7).toInt
    val algo = args(8)
    val maxBins = args(9).toInt

    // 删除输出，不在Scala算法里面删除，而在Java代码里面删除
    //     Utils.deleteOutput(output)

    val sc =  Utils.getSparkContext(testOrNot,"DecisionTree create Model")

    // construct data
    // Load and parse the data
    val training = Utils.getLabeledPointData(sc,input,minPartitions,splitter,targetIndex).cache()
    //Empty categoricalFeaturesInfo indicates all features are continuous
    val categoricalFeaturesInfo = Map[Int, Int]()
    // Run training algorithm to build the model decided by the type of algorithm
    val model = algo.toLowerCase match {
      case "classification" =>{
        val numClasses = args(10).toInt
        DecisionTree.trainClassifier(training,numClasses,categoricalFeaturesInfo,impurity,maxDepth,maxBins)
      }
      case "regression" => {
        DecisionTree.trainRegressor(training,categoricalFeaturesInfo,impurity,maxDepth,maxBins)
      }
      case _ => throw new IllegalArgumentException(s"$algo is not supported by the DecisionTree.")
    }

    // save model

    model.save(sc,output)

    sc.stop()
  }
}
