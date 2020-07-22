package com.esamusenko.task

import com.esamusenko.task.handler.{AggregationCollector, ColumnGenerator}
import com.esamusenko.task.model.{ResultElem, TransformationParams}
import com.esamusenko.task.utils.RowUtils.RowRich
import org.apache.spark.sql.SQLContext

object Processor {
  def process(params: List[TransformationParams], pathFile: String)(implicit sqlContext: SQLContext): Seq[ResultElem] = {
    val csvDF = sqlContext.read
      .option("header", value = true)
      .option("quote", "'")
      .option("ignoreLeadingWhiteSpace", value = true)
      .option("ignoreTrailingWhiteSpace", value = true)
      .csv(pathFile)

    val convertedDF = csvDF.filter(r => r.nonExistEmpty())
      .select(ColumnGenerator.generateNewColumns(params): _*)

    convertedDF.persist()
    val res = AggregationCollector.collect(convertedDF, params)
    convertedDF.unpersist()

    res
  }
}
