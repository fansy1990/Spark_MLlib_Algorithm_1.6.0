package com.fz.clustering

import com.fz.util.Utils
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors

/**
 *
 * Created by wenchao on 2017-1-16.
*/
object SparkKMeansPredict {

  def main(args: Array[String]): Unit = {
    if (args.length != 6) {
      println("algorithm.clustering.SparkKMeansPredict" +
        " <testOrNot> <inputData> <splitter> <modelFile> <outputFile> <columns>")
      sys.exit(-1)
    }

    // paramers
    val testOrNot: Boolean = args(0).toBoolean
    val inputData: String = args(1)
    val splitter :String = args(2)
    val modelFile :String = args(3)
    val outputFile: String = args(4)
    val columns:String = args(5)
    val sc =  Utils.getSparkContext(testOrNot,"Kmeans predict!")

    /**
     * 根据模型路径以及测试数据集进行预测
     *  inputData 输入测试文件
     *  splitter 输入数据分隔符
     *  outputFile 输出文件夹
     *  modelFile 模型路径
     *  columns 列选择字符串
     *
     */

    val model = KMeansModel.load(sc,modelFile)

    println("聚类数："+model.k +"")
    println("聚类中心："+model.clusterCenters.foreach(println)+"")
    val data = sc.textFile(inputData).map{
      line => line.split(splitter).map(_.toDouble)
    }

    // 在预测的时候需要指定针对选择的列的数据进行预测
    val predictLabelPoints = data.map{
      vec =>
      val preVec = Utils.getVectors(vec,columns)
        model.predict(preVec)+","+Vectors.dense(vec).toString.substring(1,vec.toString.length-1)
    }

    predictLabelPoints.saveAsTextFile(outputFile)

    sc.stop()
  }


}
