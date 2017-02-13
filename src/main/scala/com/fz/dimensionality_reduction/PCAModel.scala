package com.fz.dimensionality_reduction

import com.fz.util.Utils
import org.apache.spark.mllib.feature.PCA

/**
 * Created by fanzhe on 2017/2/13.
 */
object PCAModel {

  def main(args:Array[String]) ={
    if (args.length != 7) {
      println("com.fz.dimensionality_reduction.PCAModel " +
        " <testOrNot> <inputData> <splitter> <minPartitions> <outputFile> " +
        " <targetIndex> <k>")
      sys.exit(-1)
    }

    // paramers
    val testOrNot = args(0).toBoolean
    val inputData = args(1)
    val splitter = args(2)
    val minPartitions  = args(3).toInt
    val outputFile  = args(4)
    val targetIndex = args(5).toInt
    val k = args(6).toInt

    val sc =  Utils.getSparkContext(testOrNot,"PCA Model")

    val data = Utils.getLabeledPointData(sc,inputData,minPartitions,splitter,targetIndex).cache()

    // Compute the top 10 principal components.
    val pca = new PCA(k ).fit(data.map(_.features))

    // Project vectors to the linear space spanned by the top 10 principal components, keeping the label
    val projected = data.map(p => p.copy(features = pca.transform(p.features)))

    projected.map(x => x.features.toArray.mkString(splitter)+splitter + x.label).saveAsTextFile(outputFile)

    sc.stop()
  }

}
