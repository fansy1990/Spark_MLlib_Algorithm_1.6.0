package com.fz.classification

import com.fz.util.Utils
import org.apache.spark.mllib.tree.{RandomForest}

/**
 * 随机森林封装算法   可用于
 *      二元分类： Labels should take values {0, 1}.
 *      多元分类： Labels should take values {0, 1, ..., numClasses-1}.
 *      回归：Labels are real numbers.
 * 输入参数：
 * testOrNot : 是否是测试，正常情况设置为false
 * input：输出数据；
 * minPartitions: 输入数据最小partition个数
 * output：输出路径
 * targetIndex：目标列所在下标，从1开始
 * splitter：数据分隔符；
 * algo:分类还是回归
 * impurity：不纯度度函数，分类的有：信息熵(entropy)和基尼指数(gini)，回归有：方差（variance）
 * maxDepth：树的最大深度
 * maxBins：设定分裂数据集  suggestion 32
 * numTrees:树的数量
 * numClasses: 分类特有的：类别的个数
 *
 * featureSubsetStrategy Number of features to consider for splits at each node.
 *                              Supported: "auto", "all", "sqrt", "log2", "onethird".
 *                              If "auto" is set, this parameter is set based on numTrees:
 *                                if numTrees == 1, set to "all";
 *                                if numTrees > 1 (forest) set to "onethird".
 * Created by cuihuan on 2017/1/17.
 */
object CustomRandomForest {
  def main (args: Array[String]) {
    if(args.length != 11 && args.length != 12 ){
      println("Usage: com.fz.classification.CustomRandomForest testOrNot input minPartitions output targetIndex " +
        "splitter algo impurity maxDepth  maxBins numTrees [numClasses]<numClass only for classification> ")
      System.exit(-1)
    }
    val testOrNot = args(0).toBoolean // 是否是测试，sparkContext获取方式不一样, true 为test
    val input = args(1)
    val minPartitions = args(2).toInt
    val output = args(3)
    val targetIndex = args(4).toInt // 从1开始，不是从0开始要注意
    val splitter = args(5)
    val algo = args(6) //
    val impurity = args(7)
    val maxDepth = args(8).toInt
    val maxBins = args(9).toInt
    val numTrees = args(10).toInt

    // 删除输出，不在Scala算法里面删除，而在Java代码里面删除
    //     Utils.deleteOutput(output)

    val sc =  Utils.getSparkContext(testOrNot,"CustomRandomForest create Model")

    // construct data
    // Load and parse the data
    val training = Utils.getLabeledPointData(sc,input,minPartitions,splitter,targetIndex).cache()

    //Empty categoricalFeaturesInfo indicates all features are continuous
    val categoricalFeaturesInfo = Map[Int, Int]()
    val featureSubsetStrategy = "auto" // Let the algorithm choose.

    // Run training algorithm to build the model
    val model = algo match {
        case "classification" =>{
          val numClasses =args(11).toInt
          RandomForest.trainClassifier(training,numClasses,categoricalFeaturesInfo,numTrees,
                                      featureSubsetStrategy,impurity,maxDepth,maxBins)
        }
        case "regression" => {
          RandomForest.trainRegressor(training,categoricalFeaturesInfo,numTrees,
                                      featureSubsetStrategy, impurity,maxDepth,maxBins)
        }
        case _ => throw new RuntimeException("no regMethod specific")
      }

    // save model
    model.save(sc,output)

    sc.stop()
  }
}
