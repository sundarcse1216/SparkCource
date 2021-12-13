package com.learn

import org.apache.spark.sql.SparkSession

object CreatingSparkCSVDataFrame {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("My First CSV Dataframe application").master("local").getOrCreate()
    val properties = Map("header" -> "true", "inferSchema" -> "true")
    val df = sparkSession.read
      //      .option("header", "true")
      //      .option("inferSchema", "true")
      //      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .options(properties)
      .csv("src/main/resources/financial.csv")
    df.printSchema()
    df.show(10)
  }
}
