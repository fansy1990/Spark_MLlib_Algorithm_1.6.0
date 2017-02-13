package com.fz.clustering

import java.io.File

import com.fz.util.Utils
import org.junit.Assert._
import org.junit.Test

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
class SparkPICTest {

  @Test
  def testMain1()= {

//    <testOrNot> <inFile> <splitter> <numClusters> <numIterations> <initializationMode>
// <outFile> <columns>")

      val args = Array(
    "true",
    "./src/data/clustering/pca.txt",
    ",",
    "3",
    "10",
    "random",
    "./target/pic/tmp1",
    "111"
    )

    // 删除输出目录
    Utils.deleteOutput(args(6))
    SparkPIC.main(args)
    val exist = new File(args(6)).exists()
    assertTrue(exist)

  }

}
