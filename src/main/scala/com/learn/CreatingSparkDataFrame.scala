package com.learn

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object CreatingSparkDataFrame {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("My First Spark Data Frame Application").master("local").getOrCreate()

    val intArray = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 0)
    val intArrayRDD = sparkSession.sparkContext.parallelize(intArray)
    val schema = StructType(
      StructField("ID", IntegerType, nullable = false)
        :: Nil
    )
    val intArrayRowRDD = intArrayRDD.map(element => Row(element))
    val dataFrame = sparkSession.createDataFrame(intArrayRowRDD, schema)
    dataFrame.printSchema()
    dataFrame.show()
    dataFrame.show(5)
  }
}
