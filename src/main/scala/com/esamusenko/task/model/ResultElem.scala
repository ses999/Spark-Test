package com.esamusenko.task.model

case class ResultElem(column: String, uniqueValues: Long, values: Map[String, Long])


object ResultElem {
  def apply(column: String, values: Map[String, Long]): ResultElem = new ResultElem(column, values.values.sum, values)
}