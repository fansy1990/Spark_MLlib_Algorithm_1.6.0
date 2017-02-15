package com.fz.clustering

import java.io.File

import com.fz.util.Utils
import org.junit.Assert._
import org.junit.Test

/**
 * Created by admin on 2017-2-05.
 *
 *  GMM(Gaussian Mixture Model)封装算法
 *输入参数：
 * testOrNot : 是否是测试，true为测试，正常情况设置为false
 * inputData：输入数据，数据类型为数值型；
 * splitter：数据分隔符；
 * numClusters：聚类个数
 * numIterations : 最大迭代次数
 * seed：随机种子，默认为1，Long类型。
 * outputFile：模型输出路径
 * columns：选择的列字符串，"1"代表选择，"0"代表不选择。例如："110011",选择1,2,5,6列。
 *
 */
class SparkGMMTest {

  @Test
  def testMain1()= {

//    <testOrNot> <inFile> <splitter> <numClusters> <numIterations> <seed>
// <outFile> <columns>")

      val args = Array(
    "true",
    "./src/data/clustering/gmm.txt",
    " ",
    "3",
    "10",
    "1",
    "./target/gmm/tmp1",
    "111"
    )

    // 删除输出目录
    Utils.deleteOutput(args(6))
    SparkGMM.main(args)
    assertTrue(Utils.fileContainsClassName(args(6)+"/metadata/part-00000",
      "org.apache.spark.mllib.clustering.GaussianMixtureModel"))
  }

}
