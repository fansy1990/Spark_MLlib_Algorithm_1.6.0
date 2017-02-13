package com.fz.classification

import com.fz.util.Utils
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.optimization.L1Updater

/**
 * 算法对比
 *
 * 输入参数：
 * testOrNot : 是否是测试，正常情况设置为false
 * input：输入测试数据（包含label的数据）；
 * minPartitions: 输入数据最小partition个数
 * output：输出路径
 * targetIndex：目标列所在下标，从1开始
 * splitter : 输入数据分隔符
 * model1 : 模型1的路径
 * model2: 模型2的路径
 * Created by fanzhe on 2017/1/25.
 */
object ClassificationComparison {

   def main (args: Array[String]) {
    if(args.length != 8){
      println("Usage: com.fz.classification.ClassificationComparison testOrNot input minPartitions output targetIndex " +
        "splitter model1 model2")
      System.exit(-1)
    }
     val testOrNot = args(0).toBoolean // 是否是测试，sparkContext获取方式不一样, true 为test
     val input = args(1)
     val minPartitions = args(2).toInt
     val output = args(3)
     val targetIndex = args(4).toInt // 从1开始，不是从0开始要注意
     val splitter = args(5)
     val model1Path = args(6)
     val model2Path = args(7)

     // 删除输出，不在Scala算法里面删除，而在Java代码里面删除
     //     Utils.deleteOutput(output)

     val sc =  Utils.getSparkContext(testOrNot,"Classification Algorithm Comparison")

     // construct data
     // Load and parse the data
     val training = Utils.getLabeledPointData(sc,input,minPartitions,splitter,targetIndex).cache()

     // get model
     val model1 = Utils.getModel(sc,model1Path)

     val model2 = Utils.getModel(sc,model2Path)

     // save model

    // model.save(sc,output)

     sc.stop()
  }
}
