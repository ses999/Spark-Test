package com.esamusenko.task.handler

import com.esamusenko.task.model.TransformationParams
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.DateType

object ColumnGenerator {
  def generateNewColumns(params: Seq[TransformationParams]): Seq[Column] = {
    params.map(generateNewColumn)
  }

  def generateNewColumn(param: TransformationParams): Column = {
    val column = param.dataType match {
      case _: DateType => to_date(col(s"${param.existingColName}"), param.dateExpression.get)
      case _ => col(s"${param.existingColName}")
    }

    column.as(param.newColName).cast(param.dataType)
  }
}
