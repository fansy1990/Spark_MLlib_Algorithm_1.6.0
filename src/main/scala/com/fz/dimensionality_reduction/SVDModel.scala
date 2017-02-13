package com.fz.dimensionality_reduction

import com.fz.util.Utils
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition}
import org.apache.spark.mllib.linalg.distributed.RowMatrix

/**
  * Created by fanzhe on 2017/2/13.
  */
object SVDModel {

   def main(args:Array[String]) ={
     if (args.length != 6) {
       println("com.fz.dimensionality_reduction.SVDModel " +
         " <testOrNot> <inputData> <splitter> <minPartitions> <outputFile> " +
         " <k>")
       sys.exit(-1)
     }

     // paramers
     val testOrNot = args(0).toBoolean
     val input = args(1)
     val splitter = args(2)
     val minPartitions  = args(3).toInt
     val outputFile  = args(4)
     val k = args(5).toInt


     val sc =  Utils.getSparkContext(testOrNot,"SVD Model")

     val data = new RowMatrix(Utils.getVectorData(sc,input,splitter,minPartitions))

     // Compute the top 20 singular values and corresponding singular vectors.
     val svd: SingularValueDecomposition[RowMatrix, Matrix] = data.computeSVD(k, computeU = true)
     val U = svd.U.rows
      U.map(x => x.toArray.mkString(splitter)).saveAsTextFile(outputFile)

     sc.stop()
   }

 }
