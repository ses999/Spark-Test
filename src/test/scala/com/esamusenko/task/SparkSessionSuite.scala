package com.esamusenko.task

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionSuite {
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]")
    .setAppName("Spark CSV Transformator Test")

  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf)
    .getOrCreate()


}
