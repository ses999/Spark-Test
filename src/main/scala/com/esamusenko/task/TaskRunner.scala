package com.esamusenko.task

import com.esamusenko.task.parser.TransformationParamsParser
import com.esamusenko.task.utils.FileUtils
import org.apache.spark.sql.{SQLContext, SparkSession}

object TaskRunner {

  def main(args: Array[String]): Unit = {

    val paramPathFile = "/param.json"
    val inputPathFile = "src\\main\\resources\\sample.csv"

    val paramsJson = FileUtils.getStringFromResource(paramPathFile)
    val params = TransformationParamsParser().parseTransformationParams(paramsJson)

    val session: SparkSession = SparkSessionBuilder.build()

    implicit val sqlContext: SQLContext = session.sqlContext

    val result = Processor.process(params, inputPathFile)

    println(result)
  }

}
