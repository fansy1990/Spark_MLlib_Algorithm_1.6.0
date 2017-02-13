package com.fz.frequent_pattern_mining

import com.fz.util.Utils
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD

/**
 * Created by fanzhe on 2017/2/13.
 */
object FPModel {
  def main (args: Array[String]) {
    if (args.length != 8) {
      println("com.fz.frequent_pattern_mining.FPModel " +
        " <testOrNot> <inputData> <splitter> <minPartitions> <outputFile> " +
        " <minSupport> <numPartitions> <minConfidence>")
      sys.exit(-1)
    }

    // paramers
    val testOrNot = args(0).toBoolean
    val input = args(1)
    val splitter = args(2)
    val minPartitions  = args(3).toInt
    val outputFile  = args(4)
    val minSupport = args(5).toDouble
    val numPartitions = args(6).toInt
    val minConfidence = args(7).toDouble

    val sc =  Utils.getSparkContext(testOrNot,"Fp Model")
    val data = sc.textFile(input,minPartitions)

    val transactions: RDD[Array[String]] = data.map(s => s.trim.split(splitter))

    val fpg = new FPGrowth()
      .setMinSupport(minSupport)
      .setNumPartitions(numPartitions)

    val model = fpg.run(transactions)

    model.freqItemsets.map(x => x.items.mkString("[",",","]") + ","+ x.freq).saveAsTextFile(outputFile+"/freqItems")


    model.generateAssociationRules(minConfidence).map( rule =>

        rule.antecedent.mkString("[", ",", "]")
          + " => " + rule.consequent .mkString("[", ",", "]")
          + ", " + rule.confidence).saveAsTextFile(outputFile + "/rules")

    sc.stop()

  }
}
