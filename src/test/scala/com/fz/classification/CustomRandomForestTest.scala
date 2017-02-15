package com.fz.classification

import java.io.File

import com.fz.util.Utils
import org.junit.Assert._
import org.junit.Test

/**
 * 测试CustomRandomForest算法
 * Created by cuihuan on 2017/1/17.
 */
@Test
class CustomRandomForestTest {
  //分类
  @Test
  def testMain1()={
    //    testOrNot input minPartitions output targetIndex  +
    //splitter algo impurity maxDepth  maxBins numTrees numClasses
    val args = Array(
      "true",
      "./src/data/classification_regression/logistic.dat",
      "2",
      "./target/randomForest/tmp1",
      "1",
      " ",
      "classification",
      "gini",// 或者 entropy
      "10",
      "32",
      "5",
      "2"
    )
    // 删除输出目录
    Utils.deleteOutput(args(3))
    CustomRandomForest.main(args)
    assertTrue(Utils.fileContainsClassName(args(3)+"/metadata/part-00000",
      "org.apache.spark.mllib.tree.model.RandomForestModel"))
  }

  //回归
  @Test
  def testMain2()={
    //   testOrNot input minPartitions output targetIndex  +
    //splitter algo impurity maxDepth  maxBins numTrees numClasses
    val args = Array(
      "true",
      "./src/data/classification_regression/lpsa.dat",
      "2",
      "./target/randomForest/tmp2",
      "1",
      " ",
      "regression",
      "variance",
      "10",
      "32",
      "5",
      "2"
    )
    // 删除输出目录
    Utils.deleteOutput(args(3))
    CustomRandomForest.main(args)
    assertTrue(Utils.fileContainsClassName(args(3)+"/metadata/part-00000",
      "org.apache.spark.mllib.tree.model.RandomForestModel"))
  }
}
