package com.learn

import org.apache.spark.sql.SparkSession

case class Finance(Year: Integer, Industry_aggregation_NZSIOC: String, Industry_code_NZSIOC: String, Industry_name_NZSIOC: String,
                   Units: String, Variable_code: String, Variable_name: String, Variable_category: String, Value: String,
                   Industry_code_ANZSIC06: String)

object CreatingDataSet {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("My First CSV Dataframe application").master("local").getOrCreate()

    import sparkSession.implicits._

    val properties = Map("header" -> "true", "inferSchema" -> "true")
    val financeDS = sparkSession.read
      .options(properties)
      .csv("src/main/resources/financial.csv").as[Finance]

    financeDS.show(10)
  }
}
