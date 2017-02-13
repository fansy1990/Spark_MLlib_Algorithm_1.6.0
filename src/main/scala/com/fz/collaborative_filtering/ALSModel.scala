package com.fz.collaborative_filtering

import com.fz.util.Utils
import org.apache.spark.mllib.recommendation.{ALS, Rating}

/**
 * ALS 模型建立
 * Created by fansy on 2017/2/13.
 */
object ALSModel {
  def main(args: Array[String]) {
    if (args.length != 11) {
      println("com.fz.collaborative_filtering.ALSModel " +
        " <testOrNot> <inputData> <splitter> <minPartitions> <outputFile> <numBlocks> " +
        " <rank> <iterations> <lambda> <implicitPrefs> <alpha>")
      sys.exit(-1)
    }

    // paramers
    val testOrNot = args(0).toBoolean
    val inputData = args(1)
    val splitter = args(2)
    val minPartitions  = args(3).toInt
    val outputFile  = args(4)
    val numBlocks  = args(5).toInt
    val rank  = args(6).toInt
    val iterations = args(7).toInt
    val lambda = args(8).toDouble
    val implicitPrefs = args(9).toBoolean
    val alpha = args(10).toDouble

    val sc =  Utils.getSparkContext(testOrNot,"ALS Create Model")

    val data = Utils.getRatingData(sc,inputData,minPartitions,splitter).cache()

    val model = if(implicitPrefs){// implicit model
      ALS.trainImplicit(data, rank, iterations, lambda, numBlocks,alpha)
    }else { //
      ALS.train(data, rank, iterations, lambda,numBlocks)
    }
    model.save(sc,outputFile)

    sc.stop()
  }
}
