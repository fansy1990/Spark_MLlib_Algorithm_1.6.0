package com.fz.clustering

import com.fz.util.Utils
import org.apache.spark.mllib.clustering.PowerIterationClustering

/**
 * Created by weiwenchao on 2017-2-5.
 *
 * * PIC(PowerIterationClustering，幂迭代聚类)封装算法
 *输入参数：
 * testOrNot : 是否是测试，true为测试，正常情况设置为false
 * inputData：输入数据，模型输入数据格式为：(srcId, dstId, similarity)，且类型对应为
    (Long, Long, Double)这样的一个三元组；注意：由于算法可以进行列选择，因此原始数据不必是(srcId, dstId, similarity)格式。
    输入数据中，数据对（srcId,dstId）不管先后顺序如何只能出现至多一次（即是无向图，即两顶点边权重是唯一的）
    如果输入数据中不含这个数据对（不考虑顺序），则这两个数据对的相似度为0（没有这条边）。
 * splitter：数据分隔符；
 * numClusters：聚类个数
 * numIterations : 最大迭代次数
 * initializationMode：初始化模式，可选择 "random"或 "degree",默认为"random".(This can be either "random" to use a random vector as vertex properties, or "degree" to use normalized sum similarities. Default: random.)
 * outputFile：模型输出路径
 * columns：选择的列字符串，"1"代表选择，"0"代表不选择。例如："110011",选择1,2,5,6列。
 *
 */
object SparkPIC {

  def main(args: Array[String]) {
    if (args.length != 8) {
      println("algorithm.clustering.PIC" +
        " <testOrNot> <inputData> <splitter> <numClusters> <numIterations> <initializationMode> <outputFile> <columns>")
      sys.exit(-1)
    }

    // paramers
    val testOrNot = args(0).toBoolean
    val inputData = args(1)
    val splitter = args(2)
    val numClusters  = args(3).toInt
    val numIterations  = args(4).toInt
    val initializationMode  = args(5)
    val outputFile = args(6)
    val columns = args(7)

    val sc =  Utils.getSparkContext(testOrNot,"PIC Create Model")


    val parsedData = sc.textFile(inputData).map { line =>
      val values = line.split(splitter)
      // 使用定制的列，而非全部数据
      Utils.getTuples(values, columns)
    }.cache()

    val pic = new PowerIterationClustering().setK(numClusters)
      .setMaxIterations(numIterations).setInitializationMode(initializationMode)
      .run(parsedData)

//    输出聚类结果
    pic.assignments.foreach { a =>

      println(s"${a.id} -> ${a.cluster}")
    }

    pic.save(sc,outputFile)
    sc.stop()
  }
}
