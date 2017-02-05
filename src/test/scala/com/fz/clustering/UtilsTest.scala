package com.fz.clustering

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
/**
 * Created by admin on 2017-1-17.
 */
object UtilsTest {

  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName("woco").setMaster("local[3]")
    val sc = new SparkContext(conf)
    //    val a = (1,2,3,4,5,6,7,8)       //元组
    //    val b = List(1,2,3,4,5,6,7,8)   //集合
    //    val c = new Array[Int](6)       //数组

    //windows本地，Linux本地，hdfs 读取文件创建RDD：
//    val file = Source.fromFile("D:\\a.txt").getLines.toArray
//    val e2 = sc.parallelize(file)

//    测试向量与数组的输出

    val arr =  Array(1.0,2,3)
    val arr1 = Vector(1,2.0,3)
    val arr2 = Vectors.dense(arr)
    val arr3 = Vectors.dense(arr).toString
    println(arr)
    println(arr.toString)
    println(arr1)
    println(arr2)
    println(arr3)
    println(arr.toString.substring(1,arr.toString.length-1))
    println(arr2.toString.substring(1,arr2.toString.length-1))
    println(arr3.toString.substring(1,arr3.toString.length-1))
    println(arr.size +","+arr1.size +","+arr2.size +","+arr3.size )
//    model.predict(preVec)+","+vec.toString.substring(1,vec.toString.length-1)



  }
}
