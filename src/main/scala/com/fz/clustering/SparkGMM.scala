package com.fz.clustering

import com.fz.util.Utils
import org.apache.spark.mllib.clustering.GaussianMixture

/**
 * Created by weiwenchao on 2017-2-5.
 *
 * GMM(GaussianMixtureModel，高斯混合模型)封装算法
 *输入参数：
 * testOrNot : 是否是测试，true为测试，正常情况设置为false
 * inputData：输入数据，数据类型为数值型；
 * splitter：数据分隔符；
 * numClusters：聚类个数
 * numIterations : 最大迭代次数
 * seed：随机种子，默认为1，Long类型。
 * outputFile：模型输出路径
 * columns：选择的列字符串，"1"代表选择，"0"代表不选择。例如："110011",选择1,2,5,6列。
 *
 */
object SparkGMM {

  def main(args: Array[String]) {

    if (args.length != 8) {
      println("algorithm.clustering.GMM" +
        " <testOrNot> <inputData> <splitter> <numClusters> <numIterations> <seed> <outputFile> <columns>")
      sys.exit(-1)
    }

    // paramers
    val testOrNot: Boolean = args(0).toBoolean
    val inputData: String = args(1)
    val splitter :String = args(2)
    val numClusters :Int = args(3).toInt
    val numIterations :Int = args(4).toInt
    val seed :Long = args(5).toLong
    val outputFile: String = args(6)
    val columns :String = args(7)

    val sc =  Utils.getSparkContext(testOrNot,"GMM Create Model")


    val parsedData = sc.textFile(inputData).map { line =>
      val values = line.split(splitter).map(_.toDouble)
      // 使用定制的列，而非全部数据
      Utils.getVectors(values, columns)
    }.cache()

//    Cluster the data into  K  classes using GaussianMixture
    val gmm = new GaussianMixture().setK(numClusters)
      .setMaxIterations(numIterations).setSeed(seed).run(parsedData)

    // Save and load model
    gmm.save(sc, outputFile)
//    val sameModel = GaussianMixtureModel.load(sc, outputFile)

    // output parameters of max-likelihood model
//    for (i <- 0 until gmm.k) {
   //   println("weight=%f\nmu=%s\nsigma=\n%s\n" format
    //    (gmm.weights(i), gmm.gaussians(i).mu,gmm.gaussians(i).sigma))
   // }


//        val array = Array(9,9.0,9)
//        val cls = gmm.predict(Vectors.dense(array))   //预测每个向量所属类别
//        println("预测类别：" + cls)

    //    Given the input vector, return the membership values to all mixture components.
    //        预测每个向量隶属于每个成分（Component）的概率

    //        gmm.predictSoft(Vectors.dense(array)).foreach(println)

    sc.stop()

  }
}
