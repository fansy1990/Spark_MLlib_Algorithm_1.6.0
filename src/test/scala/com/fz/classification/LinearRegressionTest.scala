package com.fz.classification

import java.io.File

import com.fz.util.Utils
import org.junit.Assert._
import org.junit.Test

/**
 * 测试LinearRegressionTest算法
 * Created by cuihuan on 2017/1/11.
 */
@Test
class LinearRegressionTest {

  @Test
  def testMain1()={
//    testOrNot input minPartitions output targetIndex " +
//    "splitter numIteration stepSize miniBatchFraction hasIntercept
    val args = Array(
      "true",
      "./src/data/classification_regression/lpsa.dat",
      "2",
      "./target/linearRegression/tmp1",
      "1",
      " ",
      "30",
      "1.0",
      "1.0",
      "true"

    )
    // 删除输出目录
    Utils.deleteOutput(args(3))
    LinearRegression.main(args)
    assertTrue(Utils.fileContainsClassName(args(3)+"/metadata/part-00000",
      "org.apache.spark.mllib.regression.LinearRegressionModel"))
  }

}
