package com.fz.classification

import java.io.File

import com.fz.util.Utils
import org.junit.Assert._
import org.junit.Test

/**
 * 测试CustomIsotonicRegression算法封装
 * Created by cuihuan on 2017/1/16.
 */
@Test
class CustomIsotonicRegressionTest {
  @Test
  def testMain(): Unit ={
    //    testOrNot input minPartitions output targetIndex splitter weights isotonic
    val args = Array(
      "true",
      "./src/data/isotonicRegression.dat",
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
