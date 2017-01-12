package com.fz.classification

import com.fz.util.Utils
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

/**
 * 线性回归封装算法
 * 规则函数：no no regulation
 * Labels used in LinearRegression should be number(integer or double)
 * 输入参数：
 * testOrNot : 是否是测试，正常情况设置为false
 * input：输出数据；
 * minPartitions: 输入数据最小partition个数
 * output：输出路径
 * targetIndex：目标列所在下标，从1开始
 * splitter：数据分隔符；
 * numIteration：循环次数，默认100
 * stepSize : 步进，默认1.0
 * miniBatchFraction: 批处理粒度，默认1.0 Fraction of data to be used per iteration
 * hasIntercept:是否有截距{true or false}
 *
 * Created by cuihuan on 2017/1/11.
 */
object LinearRegression {
  def main(args: Array[String]) {
    if (args.length != 10) {
      println("Usage: com.fz.classification.LinearRegression testOrNot input minPartitions output targetIndex " +
        "splitter numIteration stepSize miniBatchFraction hasIntercept ")
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
    val miniBatchFraction = args(8).toDouble
    val hasIntercept = args(9).toBoolean
    // 删除输出，不在Scala算法里面删除，而在Java代码里面删除
    //     Utils.deleteOutput(output)

    val sc = Utils.getSparkContext(testOrNot, "LinearRegression create Model")

    // construct data
    // Load and parse the data
    val training = Utils.getLabeledPointData(sc, input, minPartitions, splitter, targetIndex).cache()

    // Run training algorithm to build the model
    val model = {
      val lrAlg = new LinearRegressionWithSGD()
      lrAlg.setIntercept(hasIntercept)
      lrAlg.optimizer.
        setNumIterations(numIteration).
        setStepSize(stepSize).
        setMiniBatchFraction(miniBatchFraction)
      lrAlg.run(training)
    }

    // save model

    model.save(sc, output)

    sc.stop()
  }

}
