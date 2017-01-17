package com.fz.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 * 工具类
 * Created by fansy on 2016/12/30.
 */
object Utils {

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
}
