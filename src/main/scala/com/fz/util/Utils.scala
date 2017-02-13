package com.fz.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.mllib.classification.{SVMModel, LogisticRegressionModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * 工具类
 * Created by fansy on 2016/12/30.
 */
object Utils {
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
  /**
   * 根据路径获取模型
   * @param s
   * @return
   */
  def getModel(sc:SparkContext , s: String) = {
    // 读取路径，获得类名：
    //{"class":"org.apache.spark.mllib.classification.LogisticRegressionModel",
    // "version":"1.0","numFeatures":16,"numClasses":2}
    val className = "org.apache.spark.mllib.classification.LogisticRegressionModel"
    val class_ = Class.forName(className)

    class_ match {
      case x : LogisticRegressionModel => LogisticRegressionModel.load(sc, s)
      case x : SVMModel => SVMModel.load(sc,s)
      case _ => println("根据类名加载模型异常！"); null
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
      else new SparkConf())setAppName(name)
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
