package com.fz.classification

import java.io.File

import com.fz.util.Utils
import org.junit.Assert._
import org.junit.Test

/**
 * 测试RidgeAndLassoRegression算法
 *
 * Created by cuihuan on 2017/1/12.
 */
@Test
class RidgeAndLassoRegressionTest {

  @Test
  def testMain1()={
    //    testOrNot input minPartitions output targetIndex " +
    //    "splitter numIteration stepSize regParam miniBatchFraction hasIntercept regressionType
    val args = Array(
      "true",
      "./src/data/lpsa.dat",
      "2",
      "./target/ridgeAndLassoRegression/tmp1",
      "1",
      " ",
      "30",
      "1.0",
      "0.01",
      "1.0",
      "true",
//      "RIDGE"
      "LASSO"
    )
    // 删除输出目录
    Utils.deleteOutput(args(3))
    RidgeAndLassoRegression.main(args)
    val exist = new File(args(3)).exists()
    assertTrue(exist)
  }

}
