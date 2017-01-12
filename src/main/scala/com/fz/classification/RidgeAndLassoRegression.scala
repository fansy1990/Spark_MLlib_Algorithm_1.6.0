package com.fz.classification

import com.fz.util.Utils
import org.apache.spark.mllib.regression.{LassoWithSGD, RidgeRegressionWithSGD}

/**
 * 岭回归与套索回归封装算法
 * 岭回归的规则函数：L2
 * 套索回归的规则函数：L1
 * Labels used in LinearRegression should be number(integer or double)
 * 输入参数：
 * testOrNot: 是否是测试，正常情况设置为false
 * input：输出数据；
 * minPartitions: 输入数据最小partition个数
 * output：输出路径
 * targetIndex：目标列所在下标，从1开始
 * splitter：数据分隔符；
 * numIteration：循环次数，默认100
 * stepSize: 步进，默认1.0
 * regParam:正则参数，默认0.01
 * miniBatchFraction: 批处理粒度，默认1.0 Fraction of data to be used per iteration
 * hasIntercept:是否有截距{true or false}
 * methodType:RIDGE or LASSO
 *
 * Created by cuihuan on 2017/1/12.
 */
object RidgeAndLassoRegression {
  def main(args: Array[String]) {
    if (args.length != 12) {
      println("Usage: com.fz.classification.RidgeRegression testOrNot input minPartitions output targetIndex " +
        "splitter numIteration stepSize regParam miniBatchFraction hasIntercept regressionType ")
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
    val hasIntercept = args(10).toBoolean
    val regressionType = args(11)
    // 删除输出，不在Scala算法里面删除，而在Java代码里面删除
    //     Utils.deleteOutput(output)

    val sc = Utils.getSparkContext(testOrNot, regressionType.toLowerCase+"Regression create Model")

    // construct data
    // Load and parse the data
    val training = Utils.getLabeledPointData(sc, input, minPartitions, splitter, targetIndex).cache()

    // Run training algorithm to build the model
    val model = regressionType match {
      case "RIDGE" =>
      val rg = new RidgeRegressionWithSGD()
      rg.setIntercept(hasIntercept)
      rg.optimizer.
        setNumIterations(numIteration).
        setStepSize(stepSize).
        setRegParam(regParam).
        setMiniBatchFraction(miniBatchFraction)
      rg.run(training)
      case "LASSO" =>
        val lg = new LassoWithSGD()
        lg.setIntercept(hasIntercept)
        lg.optimizer.
          setNumIterations(numIteration).
          setStepSize(stepSize).
          setRegParam(regParam).
          setMiniBatchFraction(miniBatchFraction)
        lg.run(training)
      case _ => throw new RuntimeException("no Regression specific")

    }

    // save model
    model.save(sc, output)
    sc.stop()
  }

}
