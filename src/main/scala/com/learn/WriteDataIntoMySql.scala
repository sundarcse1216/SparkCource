package com.learn

import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties

object WriteDataIntoMySql {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("My First CSV Dataframe application").master("local").getOrCreate()

    val url = "jdbc:mysql://localhost:3306"
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "Root@123")
    Class.forName("com.mysql.jdbc.Driver")

    val fileProperties = Map("header" -> "true", "inferSchema" -> "true")
    val sales_data = sparkSession.read
      .options(fileProperties)
      .csv("src/main/resources/sales_data_sample.csv")
    //    println(sales_data.count())
    //    sales_data.printSchema()
    //    sales_data.show(10)
    val table = "spark2.SALES"
    sales_data.write.mode(SaveMode.Overwrite).jdbc(url, table, properties)
  }
}
