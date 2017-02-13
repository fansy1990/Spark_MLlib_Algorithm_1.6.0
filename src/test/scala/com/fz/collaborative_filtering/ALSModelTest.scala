package com.fz.collaborative_filtering

import com.fz.util.Utils
import org.junit.Assert._
import org.junit.Test

import scala.io.Source

/**
 * Created by fansy on 2017/2/13.
 */
class ALSModelTest {
  @Test
  def testMain1()= {

    //    <testOrNot> <inputData> <splitter> <minPartitions> <outputFile> <numBlocks> " +
//    " <rank> <iterations> <lambda> <implicitPrefs> <alpha>
    val args = Array(
      "true",
      "./src/data/collaborative_filtering/als.csv",
      ",",
      "4",
      "./target/als/tmp1",
      "-1",
      "10",
      "20",
    "0.01",
    "false",
    "0.01"
    )

    // 删除输出目录
    Utils.deleteOutput(args(4))
    ALSModel.main(args)
  assertTrue(Utils.fileContainsClassName(args(4)+"/metadata/part-00000",
    "org.apache.spark.mllib.recommendation.MatrixFactorizationModel"))
  }
}
