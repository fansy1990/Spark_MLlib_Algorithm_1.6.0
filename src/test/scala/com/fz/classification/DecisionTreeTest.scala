package com.fz.classification

import java.io.File

import com.fz.util.Utils
import org.junit.Assert._
import org.junit.Test

/**
 * 测试CustomDecisionTree算法
 * Created by cuihuan on 2017/1/16.
 */
@Test
class DecisionTreeTest {
  //DecisionTree的分类
  @Test
  def testMain1()={
    //    testOrNot input minPartitions output targetIndex " +
    //    "splitter impurity maxDepth algo  maxBins numClasses
    val args = Array(
      "true",
      "./src/data/classification_regression/logistic.dat",
      "2",
      "./target/decisionTree/tmp1",
      "1",
      " ",
      "gini",// 或者 entropy
      "10",
      "classification",
      "32",
      "2"
    )
    // 删除输出目录
    Utils.deleteOutput(args(3))
    CustomDecisionTree.main(args)
    assertTrue(Utils.fileContainsClassName(args(3)+"/metadata/part-00000",
      "org.apache.spark.mllib.tree.DecisionTreeModel"))
  }

  //DecisionTree的回归
  @Test
  def testMain2()={
    //    testOrNot input minPartitions output targetIndex " +
    //    "splitter impurity maxDepth algo maxBins
    val args = Array(
      "true",
      "./src/data/classification_regression/lpsa.dat",
      "2",
      "./target/decisionTree/tmp2",
      "1",
      " ",
      "variance",
      "10",
      "regression",
      "32"
    )
    // 删除输出目录
    Utils.deleteOutput(args(3))
    CustomDecisionTree.main(args)
    assertTrue(Utils.fileContainsClassName(args(3)+"/metadata/part-00000",
      "org.apache.spark.mllib.tree.DecisionTreeModel"))
  }
}
