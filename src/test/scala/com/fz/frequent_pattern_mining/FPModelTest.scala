package com.fz.frequent_pattern_mining

import com.fz.util.Utils
import org.junit.Assert._
import org.junit.Test

/**
 * Created by fansy on 2017/2/13.
 */
class FPModelTest {
  @Test
  def testMain1()= {

    //   <testOrNot> <inputData> <splitter> <minPartitions> <outputFile> " +
//    " <minSupport> <numPartitions> <minConfidence>
    val args = Array(
      "true",
      "./src/data/frequent_pattern_mining/fpgrowth.txt",
      " ",
      "4",
      "./target/fp/tmp1",
      "0.2",
      "10",
      "0.5"

    )

    // 删除输出目录
    Utils.deleteOutput(args(4))
    FPModel.main(args)
    assertEquals(Utils.fileContainsClassName(args(4)+"/freqItems/part-00000","[z],5"),true)
  }
}
