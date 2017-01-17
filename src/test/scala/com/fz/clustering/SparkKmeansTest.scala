package com.fz.clustering

import java.io.File

import com.fz.util.Utils
import org.junit.Assert._
import org.junit.Test

/**
 * Created by admin on 2017-1-16.
 */
class SparkKMeansTest {

  @Test
  def testMain1()= {

//    <testOrNot> <inFile> <splitter> <numClusters> <numIterations>
// <outFile> <columns>")

    val args = Array(
    "true",
    "./src/data/kmeans.txt",
    " ",
    "3",
    "10",
    "./target/kmeans/tmp1",
    "111"
    )

    // 删除输出目录
    Utils.deleteOutput(args(5))
    SparkKMeans.main(args)
    val exist = new File(args(5)).exists()
    assertTrue(exist)

  }
}
