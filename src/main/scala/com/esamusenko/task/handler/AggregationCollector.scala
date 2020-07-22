package com.esamusenko.task.handler

import com.esamusenko.task.model.{ResultElem, TransformationParams}
import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Encoder, Encoders}

object AggregationCollector {
  def collect(df: DataFrame, params: Seq[TransformationParams]): Seq[ResultElem] = {
    params.map(collect(df, _))
  }

  def collect(df: DataFrame, param: TransformationParams): ResultElem = {
    implicit val enc: Encoder[(String, Long)] = Encoders.product[(String, Long)]

    val map = df.filter(col(param.newColName).isNotNull).groupBy(col(param.newColName).cast(StringType)).agg(count(param.newColName)).map(r => r.getString(0) -> r.getLong(1)).collect().toMap

    ResultElem(param.newColName, map)
  }
}
