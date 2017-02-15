package com.fz.classification

import java.io.File

import com.fz.util.Utils
import org.junit.Assert._
import org.junit.Test

/**
 * 测试CustomNaiveBayes算法封装
 * Created by cuihuan on 2017/1/7.
 */
@Test
class CustomNaiveBayesTest {
  @Test
  def testMain1()={
    //    testOrNot input output targetIndex splitter modelType lambda
    val args = Array(
      "true",
      "./src/data/classification_regression/naiveBayes1.dat",
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
    assertTrue(Utils.fileContainsClassName(args(3)+"/metadata/part-00000",
      "org.apache.spark.mllib.classification.NaiveBayesModel"))
  }

  @Test
  def testMain2()={
    //    testOrNot input output targetIndex splitter modelType lambda
    val args = Array(
      "true",
      "./src/data/classification_regression/naiveBayes2.dat",
      "2",
      "./target/bayes/tmp2",
      "1",
      " ",
      "bernoulli",
      "2.0"
    )
    // 删除输出目录
    Utils.deleteOutput(args(3))
    CustomNaiveBayes.main(args)
    assertTrue(Utils.fileContainsClassName(args(3)+"/metadata/part-00000",
      "org.apache.spark.mllib.classification.NaiveBayesModel"))
  }
}
