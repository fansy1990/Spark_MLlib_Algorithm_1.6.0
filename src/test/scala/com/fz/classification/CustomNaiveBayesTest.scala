package com.fz.classification

import java.io.File

import com.fz.util.Utils
import org.junit.Assert._
import org.junit.Test

/**
 * 测试Bayes算法
 * Created by cuihuan on 2017/1/7.
 */
@Test
class CustomNaiveBayesTest {
  @Test
  def testMain1()={
    //    testOrNot input output targetIndex splitter modelType lambda
    val args = Array(
      "true",
      "./src/data/naiveBayes1.dat",
      "2",
      "./target/bayes/tmp1",
      "1",
      " ",
      "multinomial",
      "1.0"
    )
    // 删除输出目录
    Utils.deleteOutput(args(3))
    CustomNaiveBayes.main(args)
    val exist = new File(args(3)).exists()
    assertTrue(exist)
  }

  @Test
  def testMain2()={
    //    testOrNot input output targetIndex splitter modelType lambda
    val args = Array(
      "true",
      "./src/data/naiveBayes2.dat",
      "2",
      "./target/bayes/tmp1",
      "1",
      " ",
      "bernoulli",
      "2.0"
    )
    // 删除输出目录
    Utils.deleteOutput(args(3))
    CustomNaiveBayes.main(args)
    val exist = new File(args(3)).exists()
    assertTrue(exist)
  }
}
