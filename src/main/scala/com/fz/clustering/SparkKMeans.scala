package com.fz.clustering

import com.fz.util.Utils
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
/**
 * Created by wenchao on 2017-1-16.
 *
 * Kmeans封装算法
 * In data mining（数据挖掘）, k-means++ is an algorithm for choosing the initial values (or "seeds") for the k-means clustering algorithm.
 *输入参数：
 * testOrNot : 是否是测试，true为测试，正常情况设置为false
 * inputData：输入数据，数据类型为数值型；
 * splitter：数据分隔符；
 * numClusters：聚类个数
 * numIterations : 最大迭代次数
 * outputFile：模型输出路径
 * columns：选择的列字符串，"1"代表选择，"0"代表不选择。例如："110011",选择1,2,5,6列。
 *
 */
object SparkKMeans {

  def main(args: Array[String]) {

    if (args.length != 7) {
      println("algorithm.clustering.KMeans" +
        " <testOrNot> <inputData> <splitter> <numClusters> <numIterations> <outputFile> <columns>")
      sys.exit(-1)
    }

    // paramers
    val testOrNot: Boolean = args(0).toBoolean
    val inputData: String = args(1)
    val splitter :String = args(2)
    val numClusters :Int = args(3).toInt
    val numIterations :Int = args(4).toInt
    val outputFile: String = args(5)
    val columns :String = args(6)

    val sc =  Utils.getSparkContext(testOrNot,"Kmeans Create Model")


    val parsedData = sc.textFile(inputData).map { line =>
      val values = line.split(splitter).map(_.toDouble)
      // 使用定制的列，而非全部数据
      Utils.getVectors(values, columns)
    }.cache()

    val clusters: KMeansModel = KMeans.train(parsedData,numClusters,numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    // val WSSSE: Double = clusters.computeCost(parsedData)
    //      println("Within Set Sum of Squared Errors = " + WSSSE)

    //    println("聚类数： "+ clusters.k)
    //    println("聚类中心： ")
    //    clusters.clusterCenters.map(_.toArray).foreach{t =>
    //      for(i <- 0 until(t.length)){
    //        print(t(i)+",")
    //      }
    //      println("")
    //    }

    //    val vec = Array(8.0,7,9)
    //    val cls = clusters.predict(Vectors.dense(vec))
    //
    //    println("预测类别：" + cls)
    //
    //    clusters.predict(parsedData).foreach(t => println("归为： "+t))
    clusters.save(sc,outputFile)

    sc.stop()
  }

}
