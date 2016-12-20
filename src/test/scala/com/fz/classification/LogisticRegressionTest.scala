package com.fz.classification

import java.io.File

import org.junit.{Assert, Test}
import Assert._
/**
 * 测试Logistics Regression算法
 * Created by fanzhe on 2016/12/19.
 */
@Test
class LogisticRegressionTest {

  @Test
  def testMain()={
//    testOrNot input output targetIndex splitter method hasIntercept numClasses
    val args = Array(
      "true",
      "./src/data/logistic.dat",
      "./target/tmp",
      "1",
      " ",
      "SGD",
      "true",
      "2"
    )
    LogisticRegression.main(args)
    val exist = new File(args(2)).exists()
    assertTrue(exist)
  }
}
