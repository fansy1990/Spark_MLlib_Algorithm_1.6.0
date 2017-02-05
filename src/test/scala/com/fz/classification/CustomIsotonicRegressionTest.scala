package com.fz.classification

import java.io.File

import com.fz.util.Utils
import org.junit.Assert._
import org.junit.Test

/**
 * 测试算法CustomIsotonicRegression算法
 * Created by cuihuan on 2017/2/5.
 *
 */
class CustomIsotonicRegressionTest {

  @Test
  def testMain1()={
    //    testOrNot input minPartitions output targetIndex splitter weights isotonic
    val args = Array(
      "true",
      "./src/data/classification_regression/isotonicRegression.dat",
      "2",
      "./target/isotonicRegression/tmp1",
      "1",
      " ",
      "1.0",
      "true"
    )
    // 删除输出目录
    Utils.deleteOutput(args(3))
    CustomIsotonicRegression.main(args)
    val exist = new File(args(3)).exists()
    assertTrue(exist)
  }

}
