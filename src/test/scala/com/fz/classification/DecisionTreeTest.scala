package com.fz.classification

import java.io.File

import com.fz.util.Utils
import org.junit.Assert._
import org.junit.Test

/**
 * 测试DecisionTree算法
 * Created by cuihuan on 2017/1/16.
 */
@Test
class DecisionTreeTest {
  //DecisionTree的分类
  @Test
  def testMain1()={
    //    testOrNot input minPartitions output targetIndex " +
    //    "splitter impurity maxDepth numClasses maxBins
    val args = Array(
      "true",
      "./src/data/logistic.dat",
      "2",
      "./target/decisionTree/tmp1",
      "1",
      " ",
      "gini",// 或者 entropy
      "10",
      "2" ,//
      "32"
    )
    // 删除输出目录
    Utils.deleteOutput(args(3))
    DecisionTreeClassification.main(args)
    val exist = new File(args(3)).exists()
    assertTrue(exist)
  }

  //DecisionTree的回归
  @Test
  def testMain2()={
    //    testOrNot input minPartitions output targetIndex " +
    //    "splitter impurity maxDepth maxBins
    val args = Array(
      "true",
      "./src/data/lpsa.dat",
      "2",
      "./target/decisionTree/tmp2",
      "1",
      " ",
      "variance",
      "10",
      "32"
    )
    // 删除输出目录
    Utils.deleteOutput(args(3))
    DecisionTreeRegression.main(args)
    val exist = new File(args(3)).exists()
    assertTrue(exist)
  }
}
