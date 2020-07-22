package com.esamusenko.task

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionBuilder {
  def build(): SparkSession = {
    SparkSession.builder().config(getSparkConf)
      .getOrCreate()
  }

  private def getSparkConf = {
    new SparkConf().setMaster("local[2]")
      .setAppName("Spark CSV Transformator")
  }
}
