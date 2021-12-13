package com.learn

import org.apache.spark.{SparkConf, SparkContext}

object CreatingSparkContext {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("My First Spark Application")
    sparkConf.setMaster("local")

    val sc = new SparkContext(sparkConf)

    val intArray = Array(1,2,3,4,5,6,7,8,9,0)

    val intArrayRDD = sc.parallelize(intArray,3)
    println("No.of elements in intArrayRDD: ", intArrayRDD.count())
    println("No.of partition in intArrayRDD : ", intArrayRDD.partitions.length)
    intArrayRDD.foreach(println)
  }

}