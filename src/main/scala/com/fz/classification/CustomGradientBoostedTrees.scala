package com.fz.classification

import com.fz.util.Utils
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.loss.{SquaredError, AbsoluteError, LogLoss}

/**
 *梯度提升树封装算法   可用于
 *      二元分类： Labels should take values {0, 1}.
 *      spark 1.6.0 暂不支持多元分类
 *      回归：Labels are real numbers.
 * 输入参数：
 * testOrNot : 是否是测试，正常情况设置为false
 * input：输出数据；
 * minPartitions: 输入数据最小partition个数
 * output：输出路径
 * targetIndex：目标列所在下标，从1开始
 * splitter：数据分隔符；
 * algo:分类还是回归(Classification OR Regression)
 * lossType：LogLoss(Classification),AbsoluteError(Regression),SquaredError(Regression)
 * numIteration:迭代次数
 * maxDepth：树的最大深度
 *
 * numClass:分类中类别的个数，分类专有,必须为2
 *
 *
 * Created by cuihuan on 2017/1/17.
 */
object CustomGradientBoostedTrees {
  def main (args: Array[String]) {
    if(args.length != 10 && args.length != 11 ){
      println("Usage: com.fz.classification.CustomGradientBoostedTrees testOrNot input minPartitions output targetIndex " +
        "splitter algo lossType numIteration  maxDepth [numClasses]<numClass only for classification> ")
      System.exit(-1)
    }
    val testOrNot = args(0).toBoolean // 是否是测试，sparkContext获取方式不一样, true 为test
    val input = args(1)
    val minPartitions = args(2).toInt
    val output = args(3)
    val targetIndex = args(4).toInt // 从1开始，不是从0开始要注意
    val splitter = args(5)
    val algo = args(6) //
    val lossType = args(7)
    val numIteration = args(8).toInt
    val maxDepth = args(9).toInt



    // 删除输出，不在Scala算法里面删除，而在Java代码里面删除
    //     Utils.deleteOutput(output)

    val sc =  Utils.getSparkContext(testOrNot,"CustomGradientBoostedTrees create Model")

    // construct data
    // Load and parse the data
    val training = Utils.getLabeledPointData(sc,input,minPartitions,splitter,targetIndex).cache()

    //Empty categoricalFeaturesInfo indicates all features are continuous
    val categoricalFeaturesInfo = Map[Int, Int]()


    // Run training algorithm to build the model
    val model = algo.toLowerCase match {
      case "classification" =>{
        val boostingStrategy = BoostingStrategy.defaultParams("Classification")
        val numClasses =args(10).toInt
//        boostingStrategy.treeStrategy.setCategoricalFeaturesInfo(categoricalFeaturesInfo)
        boostingStrategy.treeStrategy.setNumClasses(numClasses)
        boostingStrategy.setNumIterations(numIteration)
        lossType.toUpperCase match {
          case "LOGLOSS" => boostingStrategy.setLoss(LogLoss)
          case _ => throw new IllegalArgumentException(s"The loss of $lossType is not supported by the gradient boosting.")
        }
        boostingStrategy.treeStrategy.setMaxDepth(maxDepth)

        GradientBoostedTrees.train(training,boostingStrategy)
      }
      case "regression" => {
        val boostingStrategy = BoostingStrategy.defaultParams("Regression")
        boostingStrategy.setNumIterations(numIteration)
        lossType.toUpperCase match {
          case "ABSOLUTEERROR" => boostingStrategy.setLoss(AbsoluteError)
          case "SQUAREDERROR" =>  boostingStrategy.setLoss(SquaredError)
          case _ =>  throw new IllegalArgumentException(s"The loss of $lossType is not supported by the gradient boosting.")
        }
        boostingStrategy.treeStrategy.setMaxDepth(maxDepth)
//        boostingStrategy.treeStrategy.setCategoricalFeaturesInfo(categoricalFeaturesInfo)
        GradientBoostedTrees.train(training,boostingStrategy)
      }
      case _ => throw new IllegalArgumentException(s"$algo is not supported by the gradient boosting.")
    }

    // save model
    model.save(sc,output)

    sc.stop()


  }
}

