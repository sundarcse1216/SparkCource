package com.learn

import org.apache.spark.sql.SparkSession

object CreatingTempTable {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("My First CSV Dataframe application").master("local").getOrCreate()
    val properties = Map("header" -> "true", "inferSchema" -> "true")
    val df = sparkSession.read
      .options(properties)
      .csv("src/main/resources/homes.csv")
    df.createTempView("homes")
    val limitData = sparkSession.sql("select * from homes limit 10")
    limitData.printSchema()
    limitData.show()
  }
}
