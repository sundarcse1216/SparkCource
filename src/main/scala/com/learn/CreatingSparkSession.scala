package com.learn

import org.apache.spark.sql.SparkSession


object CreatingSparkSession {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("My First Spark Session Application").master("local").getOrCreate()

    val intArray = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 0)

    val intArrayRDD = sparkSession.sparkContext.parallelize(intArray, 3)
    println("No.of elements in intArrayRDD: ", intArrayRDD.count())
    println("No.of partition in intArrayRDD : ", intArrayRDD.partitions.length)
    intArrayRDD.foreach(println)
  }
}