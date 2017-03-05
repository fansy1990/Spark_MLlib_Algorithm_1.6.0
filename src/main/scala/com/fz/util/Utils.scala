package com.fz.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.mllib.classification.{NaiveBayesModel, SVMModel, LogisticRegressionModel}
import org.apache.spark.mllib.evaluation.{MulticlassMetrics, BinaryClassificationMetrics}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.{DecisionTreeModel, RandomForestModel, GradientBoostedTreesModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * 工具类
 * Created by fansy on 2016/12/30.
 */
object Utils {
  /**
   * 根据预测结果进行评价
   * @param predictionAndLabels
   * @param numClasses
   * @return
   */
  def evaluate(predictionAndLabels: RDD[(Double, Double)], numClasses: Int) : String = {
    if(numClasses ==2){
      val metrics=new BinaryClassificationMetrics(predictionAndLabels)
      "Precision-Recall Area: "+ metrics.areaUnderPR() + "\n" +
      "Roc Area: "+ metrics.areaUnderROC() + "\n" +
      "Precision And Recall Curve:\n" +
      "precision -> recall"+ "\n" +
      metrics.pr().collect().map(x => x._1 + " -> "+x._2).mkString("\n").toString +
      "ROC Curve:\n" +
        "false positive rate -> true positive rate"+ "\n" +
        metrics.roc().collect().map(x => x._1 + " -> "+x._2).mkString("\n") +"\n"+
      "Threshold : precision , recall , f1\n" +
      (
        metrics.precisionByThreshold() zip
          metrics.recallByThreshold() zip
          metrics.fMeasureByThreshold()
        ).map(x =>  x._1._1._1 + " : " + x._1._1._2+" , " + x._1._2._2 +" , " + x._2._2).collect().
        mkString("\n")
    }else {
      val metrics = new MulticlassMetrics(predictionAndLabels)
      "Precision: " + metrics.precision+
      "\nRecall: " +metrics.recall +
      "\nConfusion metrics: \n" + metrics.confusionMatrix
    }

  }


  /**
   * 获取vector rdd数据
   * @param sc
   * @param input
   * @param splitter
   * @param minPartitions
   * @return
   */
  def getVectorData(sc: SparkContext, input: String, splitter: String, minPartitions: Int): RDD[Vector] ={
     sc.textFile(input,minPartitions).map{x => val arr = x.split(splitter);Vectors.dense(arr.map(_.toDouble))}
  }

  /**
   * 寻找文件的纬度
   * @param file
   * @param splitter
   * @return
   */
  def findDimension(file:String, splitter:String): Int ={
    Source.fromFile(file).getLines.next().split(splitter).size
  }


  def metadataPath(path: String): String = new Path(path, "metadata").toUri.toString

  /**
   * 根据模型路径获取模型相关信息
   * @param sc
   * @param path
   * @param param
   * @return
   */
  def modelParam(sc:SparkContext,path: String , param:String) = {
    //{"class":"org.apache.spark.mllib.classification.LogisticRegressionModel",
    // "version":"1.0","numFeatures":16,"numClasses":2}
    println(param +"," + path)
    implicit val formats = DefaultFormats
    val metadata = parse(sc.textFile(metadataPath(path)).first())
    (metadata \ param).extract[String]
  }
  /**
   * 根据路径获取模型,并进行预测，返回预测结果
   * @param modelPath
   * @param sc
   * @param data
   * @return
   */
  def useModel2Predict(sc:SparkContext , modelPath: String,data:RDD[LabeledPoint]):RDD[(Double,Double)] = {
    // 读取路径，获得类名：

    val className = modelParam(sc,modelPath,"class")
    print(className)
    className match {
      case "org.apache.spark.mllib.classification.LogisticRegressionModel" => {
         val model = LogisticRegressionModel.load(sc, modelPath).clearThreshold()
          data.map{case LabeledPoint(label, features)=>
          val prediction= model.predict(features)
          (prediction, label)
        }
      }
      case "org.apache.spark.mllib.classification.SVMModel" => {
        val model = SVMModel.load(sc,modelPath).clearThreshold()
        data.map{case LabeledPoint(label, features)=>
          val prediction= model.predict(features)
          (prediction, label)
        }
      }
      case "org.apache.spark.mllib.tree.DecisionTreeModel" => { // TODO tree.model.DecisionTreeModel 这里是bug还是？
        val model = DecisionTreeModel.load(sc,modelPath)

        data.map{case LabeledPoint(label, features)=>
          val prediction= model.predict(features)
          (prediction, label)
        }
      }
      case "org.apache.spark.mllib.tree.model" => {
        val model =GradientBoostedTreesModel.load(sc,modelPath)
        data.map{case LabeledPoint(label, features)=>
          val prediction= model.predict(features)
          (prediction, label)
        }
      }
      case "org.apache.spark.mllib.classification.NaiveBayesModel" => {
        val model =NaiveBayesModel.load(sc,modelPath)
        data.map{case LabeledPoint(label, features)=>
          val prediction= model.predict(features)
          (prediction, label)
        }
      }
      case "org.apache.spark.mllib.tree.model.RandomForestModel" => {
        val model = RandomForestModel.load(sc,modelPath)
        data.map{case LabeledPoint(label, features)=>
          val prediction= model.predict(features)
          (prediction, label)
        }
      }
      case _ => println("根据类名加载模型异常！")
        throw new RuntimeException("根据类名加载模型异常！")
    }

  }


  /**
   * 从输入数据获取Rating数据
   * @param sc
   * @param input
   * @param minPartitions
   * @param splitter
   * @return
   */
  def getRatingData(sc: SparkContext, input:String,minPartitions :Int,splitter:String) = {
    val data = sc.textFile(input,minPartitions)
    data.map(_.split(splitter) match { case Array(user, item, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)})
  }

  /**
   * 文件是否包含固定字符串
   * @param file
   * @param className
   * @return
   */
  def fileContainsClassName(file:String ,className:String):Boolean = {
    val fileContents = Source.fromFile(file).getLines.mkString
    fileContents.contains(className)
  }
  /**
   * 获取SparkContext
   * @param testOrNot
   * @param name
   * @return
   */
  def getSparkContext(testOrNot: Boolean,name :String) =
    new SparkContext( (if(testOrNot) new SparkConf().setMaster("local[2]")
      else new SparkConf().set("spark.driver.allowMultipleContexts","true"))setAppName(name)
    )

  /**
   * 删除输出目录
   * @param path
   * @return
   */
  def deleteOutput(path:String) = FileSystem.get(new Configuration()).delete(new Path(path),true)

  /**
   * 获取输入数据的 LabeledPoint数据
   * @param sc
   * @param input
   * @param minPartitions
   * @param splitter
   * @param targetIndex
   * @return
   */
  def getLabeledPointData(sc:SparkContext,input:String,minPartitions:Int,splitter:String,targetIndex:Int) =
    sc.textFile(input).map { line =>
    val parts = line.split(splitter)
    LabeledPoint(parts(targetIndex-1).toDouble,
      Vectors.dense( Array.concat(parts.take(targetIndex-1), parts.drop(targetIndex)).map(_.toDouble)))
  }

  /**
   * 根据列字符串选择列输出vector
   * @param ds
   * @param columns 类似"0101110" 的字符串
   * @return
   */
  def getVectors(ds:Array[Double],columns:String):Vector={
    val cols = columns.toCharArray
    val vec = ArrayBuffer[Double]()
    for(i <- 0 until(cols.length)){
      if('1'.equals(cols(i))){
        vec+=ds(i)
      }
    }
    Vectors.dense(vec.toArray)

  }

  /**
  根据列字符串输出元组
  @param ds
  @param columns 类似"0101110" 的字符串
  @return
   */
  def getTuples(ds:Array[String],columns:String)={
    val cols = columns.toCharArray
    val vec = ArrayBuffer[String]()
    for(i <- 0 until(cols.length)){
      if('1'.equals(cols(i))){
        vec+=ds(i)
      }
    }
    (vec(0).toLong,vec(1).toLong,vec(2).toDouble)
  }


//  def main(args: Array[String]) {
//    val a = Array("123","234","345","456")
//    val b = "1011"
//    val t = getTuples(a,b)
//    println(t._1+","+t._2+","+t._3)
//  }
}
