package com.fz.classification

import com.fz.util.Utils

/**
 * 分类算法对比，包括SVM，逻辑回归，随机森林，GradientBoostedTree，
 * NaiveBayes
 *
 * 模型路径： /path/to/model/output, 下面的模型路径指的就是这一级
 *              --data
 *                  -- _SUCCESS
 *                  -- _common_metadata
 *                  -- _metadata
 *                  -- part-r-....
 *              --metadata
 *                  -- _SUCCESS
 *                  -- part-00000
 *
 * 输入参数：
 * testOrNot : 是否是测试，正常情况设置为false
 * input：输入测试数据（包含label的数据）；
 * minPartitions: 输入数据最小partition个数
 * output：输出路径
 * targetIndex：目标列所在下标，从1开始
 * splitter : 输入数据分隔符
 * model : 模型的路径
 * numClasses : 模型目标类别个数
 *
 * Created by fanzhe on 2017/3/5.
 */
object ClassificationModelEvaluation {

   def main (args: Array[String]) {
    if(args.length != 8){
      println("Usage: com.fz.classification.ClassificationModelEvaluation testOrNot input minPartitions output " +
        "targetIndex splitter model numClasses")

      System.exit(-1)
    }
     val testOrNot = args(0).toBoolean // 是否是测试，sparkContext获取方式不一样, true 为test
     val input = args(1)
     val minPartitions = args(2).toInt
     val output = args(3)
     val targetIndex = args(4).toInt // 从1开始，不是从0开始要注意
     val splitter = args(5)
     val modelPath = args(6)
     val numClasses = args(7).toInt


     val sc =  Utils.getSparkContext(testOrNot,"Classification Algorithm Evaluation")

     // construct data
     // Load and parse the data
     val training = Utils.getLabeledPointData(sc,input,minPartitions,splitter,targetIndex).cache()
//     val numCLasses = Utils.modelParam(sc,modelPath,"numClasses").toInt

     // use model to predict
     val preAndReal = Utils.useModel2Predict(sc,modelPath,training)

     // model1 evaluation
     val evaluate1 = Utils.evaluate(preAndReal,numClasses)

     // save result
     sc.parallelize(Array(evaluate1),1).saveAsTextFile(output+"/evaluation")

     sc.stop()
  }
}
