package com.fz.classification

import java.io.File

import com.fz.util.Utils
import org.junit.{Assert, Test}
import Assert._
/**
 * 测试Logistics Regression算法
 * Created by fanzhe on 2016/12/19.
 */
@Test
class LogisticRegressionTest {

  @Test
  def testMain1()={
//    testOrNot input output targetIndex splitter method hasIntercept numClasses
    val args = Array(
      "true",
      "./src/data/logistic.dat",
      "2",
      "./target/logistic/tmp1",
      "1",
      " ",
      "SGD",
      "true",
      "2" // this parameter is useless
    )
    // 删除输出目录
    Utils.deleteOutput(args(3))
    LogisticRegression.main(args)
    val exist = new File(args(3)).exists()
    assertTrue(exist)
  }

  @Test
  def testMain2()={
    //    testOrNot input minPartitions output targetIndex splitter method hasIntercept numClasses
    val args = Array(
      "true",
      "./src/data/logistic.dat",
      "2",
      "./target/logistic/tmp2",
      "1",
      " ",
      "LBFGS",
      "true",
      "2"
    )
    // 删除输出目录
    Utils.deleteOutput(args(3))
    LogisticRegression.main(args)
    val exist = new File(args(3)).exists()
    assertTrue(exist)
  }
}
