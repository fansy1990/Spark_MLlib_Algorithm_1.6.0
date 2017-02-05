package com.fz.classification

import java.io.File

import com.fz.util.Utils
import org.junit.Assert._
import org.junit.{Assert, Test}

/**
 * 测试SVM算法
 * Created by fanzhe on 2016/12/30.
 */
@Test
class SVMTest {

  @Test
  def testMain1()={
//    testOrNot input minPartitions output targetIndex " +
//    "splitter numIteration stepSize regParam miniBatchFraction regMethod
    val args = Array(
      "true",
      "./src/data/classification_regression/logistic.dat",
      "2",
      "./target/svm/tmp1",
      "1",
      " ",
      "30",
      "1.0",
      "0.01" ,//
      "1.0",
      "L1"
    )
    // 删除输出目录
    Utils.deleteOutput(args(3))
    SVM.main(args)
    val exist = new File(args(3)).exists()
    assertTrue(exist)
  }

  @Test
  def testMain2()={
    //    testOrNot input minPartitions output targetIndex " +
    //    "splitter numIteration stepSize regParam miniBatchFraction regMethod
    val args = Array(
      "true",
      "./src/data/classification_regression/logistic.dat",
      "2",
      "./target/svm/tmp1",
      "1",
      " ",
      "30",
      "1.0",
      "0.01" ,//
      "1.0",
      "L2"
    )
    // 删除输出目录
    Utils.deleteOutput(args(3))
    SVM.main(args)
    val exist = new File(args(3)).exists()
    assertTrue(exist)
  }
}
