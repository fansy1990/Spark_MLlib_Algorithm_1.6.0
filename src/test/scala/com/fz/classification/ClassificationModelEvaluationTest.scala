package com.fz.classification

import com.fz.util.Utils
import org.junit.Assert._
import org.junit.Test

/**
 * Classification Model Evaluation
 * Created by fansy on 2017/3/5.
 */
@Test
class ClassificationModelEvaluationTest {
  //Decision Tree evaluation
  @Test
  def testMain1()={
    //    testOrNot input minPartitions output " +
    // "targetIndex splitter model numClasses"
    val args = Array(
      "true",
      "./src/data/classification_regression/logistic.dat",
      "2",
      "./target/decisionTree/evaluation1",
      "1",
      " ",
      "./target/decisionTree/tmp1",
      "2"
    )
    // 删除输出目录
    Utils.deleteOutput(args(3))
    ClassificationModelEvaluation.main(args)
    assertTrue(Utils.fileContainsClassName(args(3)+"/evaluation/part-00000",
      "Precision-Recall Area"))
  }

  @Test
  def testMain2()={
    //    testOrNot input minPartitions output " +
    // "targetIndex splitter model numClasses"
    val args = Array(
      "true",
      "./src/data/classification_regression/logistic.dat",
      "2",
      "./target/decisionTree/evaluation2",
      "1",
      " ",
      "./target/decisionTree/tmp1",
      "3"
    )
    // 删除输出目录
    Utils.deleteOutput(args(3))
    ClassificationModelEvaluation.main(args)
    assertTrue(Utils.fileContainsClassName(args(3)+"/evaluation/part-00000",
      "Confusion metrics"))
  }

}
