package com.fz.classification

import java.io.File

import com.fz.util.Utils
import org.junit.Assert._
import org.junit.Test

/**
 *测试CustomGradientBoostedTrees封装算法
 * Created by cuihuan on 2017/1/18.
 */
@Test
class CustomGradientBoostedTreesTest {
  //分类
  @Test
  def testMain1()={
    //    testOrNot input minPartitions output targetIndex " +
    //"splitter algo lossType numIteration  maxDepth [numClasses]<numClass only for classification>
    val args = Array(
      "true",
      "./src/data/classification_regression/logistic.dat",
      "2",
      "./target/gradientBoostedTrees/tmp1",
      "1",
      " ",
      "classification",
      "LOGLOSS",// 或者 entropy
      "10",
      "6",
      "2"
    )
    // 删除输出目录
    Utils.deleteOutput(args(3))
    CustomGradientBoostedTrees.main(args)
    assertTrue(Utils.fileContainsClassName(args(3)+"/metadata/part-00000",
      "org.apache.spark.mllib.tree.model.GradientBoostedTreesModel"))
  }

  //回归
  @Test
  def testMain2()={
    //  testOrNot input minPartitions output targetIndex " +
    //"splitter algo lossType numIteration  maxDepth
    val args = Array(
      "true",
      "./src/data/classification_regression/lpsa.dat",
      "2",
      "./target/gradientBoostedTrees/tmp2",
      "1",
      " ",
      "regression",
      "AbsoluteError",
      "10",
      "7"

    )
    // 删除输出目录
    Utils.deleteOutput(args(3))
    CustomGradientBoostedTrees.main(args)
    assertTrue(Utils.fileContainsClassName(args(3)+"/metadata/part-00000",
      "org.apache.spark.mllib.tree.model.GradientBoostedTreesModel"))
  }
}

