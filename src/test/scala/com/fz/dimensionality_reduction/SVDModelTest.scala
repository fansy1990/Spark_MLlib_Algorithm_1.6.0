package com.fz.dimensionality_reduction

import com.fz.util.Utils
import org.junit.Assert._
import org.junit.Test

/**
 * Created by fansy on 2017/2/13.
 */
class SVDModelTest {
  @Test
  def testMain1()= {

    //   <testOrNot> <inputData> <splitter> <minPartitions> <outputFile> " +
//    " <k>
    val args = Array(
      "true",
      "./src/data/dimensionality_reduction/svd.txt",
      " ",
      "4",
      "./target/svd/tmp1",
      "5"

    )

    // 删除输出目录
    Utils.deleteOutput(args(4))
    SVDModel.main(args)
    assertEquals(Utils.findDimension(args(4)+"/part-00000",args(2)),args(5).toInt)
  }
}
