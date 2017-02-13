package com.fz.dimensionality_reduction

import com.fz.util.Utils
import org.junit.Assert._
import org.junit.Test

/**
 * Created by fansy on 2017/2/13.
 */
class PCAModelTest {
  @Test
  def testMain1()= {

    //   <testOrNot> <inputData> <splitter> <minPartitions> <outputFile> " +
//    " <targetIndex> <k>
    val args = Array(
      "true",
      "./src/data/dimensionality_reduction/pca.txt",
      " ",
      "4",
      "./target/pca/tmp1",
      "1",
      "5"

    )

    // 删除输出目录
    Utils.deleteOutput(args(4))
    PCAModel.main(args)
    assertEquals(Utils.findDimension(args(4)+"/part-00000",args(2)),args(6).toInt+1)
  }
}
