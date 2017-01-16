package com.fz.classification

import com.fz.util.Utils
import org.apache.spark.mllib.regression.IsotonicRegression

/**
 *
 *保序回归封装算法
 * 现在spark1.6只支持一个特征值，即feature的个数为一个(label feature)
 *
 *
 * Labels used in CustomIsotonicRegression should be number(integer or double)
 * 输入参数：
 * testOrNot: 是否是测试，正常情况设置为false
 * input：输出数据；
 * minPartitions: 输入数据最小partition个数
 * output：输出路径
 * targetIndex：目标列所在下标，从1开始
 * splitter：数据分隔符；
 * weights：权重,正数，默认为1.0
 * isotonic：默认为true，表示函数单调递增，false表示单调递减
 *
 *
 * Created by cuihuan on 2017/1/13.
 */
object CustomIsotonicRegression {
  def main(args: Array[String]) {
    if(args.length != 8){
      println("Usage: com.fz.classification.CustomIsotonicRegression testOrNot input minPartitions output targetIndex " +
        "splitter weights isotonic")
      System.exit(-1)
    }
    val testOrNot = args(0).toBoolean // 是否是测试，sparkContext获取方式不一样, true 为test
    val input = args(1)
    val minPartitions = args(2).toInt
    val output = args(3)
    val targetIndex = args(4).toInt // 从1开始，不是从0开始要注意
    val splitter = args(5)
    val weights = args(6).toDouble
    val isotonic = args(7).toBoolean
    // 删除输出，不在Scala算法里面删除，而在Java代码里面删除
    //     Utils.deleteOutput(output)

    val sc =  Utils.getSparkContext(testOrNot,"CustomIsotonicRegression create Model")

    // construct data
    // Load and parse the data to (label,feature,weights)
    val training = sc.textFile(input,minPartitions).map(line => line.split(splitter)).map(fields => (fields(targetIndex-1).toDouble,fields(fields.length-targetIndex).toDouble,weights))
    val model = new IsotonicRegression().setIsotonic(isotonic).run(training)
    model.save(sc,output)

    sc.stop()
  }
}
