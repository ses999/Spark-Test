package com.esamusenko.task.model

import com.esamusenko.task.exception.IncorrectParamsException
import org.apache.spark.sql.types._

case class TransformationParams(existingColName: String, newColName: String, dataType: DataType, dateExpression: Option[String]) {
}

object TransformationParams {
  val mapping = Map(
    "string" -> StringType,
    "integer" -> IntegerType,
    "boolean" -> BooleanType,
    "date" -> DateType
  )

  def apply(existingColName: String, newColName: String, newDataType: String, dateExpression: Option[String]): TransformationParams = {
    val newDataTypeLower = newDataType.toLowerCase
    if (!mapping.contains(newDataTypeLower))
      throw new UnsupportedOperationException(s"DataType $newDataType is not supported")

    val dataType = mapping(newDataTypeLower)

    if (dataType.getClass == classOf[DateType] && dateExpression.isEmpty)
      throw new IncorrectParamsException("You have to define field date_expression if you use type date")

    TransformationParams(existingColName, newColName, dataType, dateExpression)
  }
}