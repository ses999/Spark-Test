package com.esamusenko.task.parser

import com.esamusenko.task.exception.IncorrectParamsException
import com.esamusenko.task.utils.FileUtils
import org.scalatest.{FunSuite, Matchers}

class TransformationParamsJsonParserSpec extends FunSuite with Matchers {

  val correctJson: String = FileUtils.getStringFromResource("/param.json")

  test("parse correct json. Size should be 3") {
    val res = TransformationParamsParser().parseTransformationParams(correctJson)

    res.size shouldBe 3
  }

  test("should throw IncorrectParamsException if json is JObject.") {
    val incorrectJson = "{\"existing_col_name\" : \"name\", \"new_col_name\" : \"first_name\", \"new_data_type\" : \"string\"}"

    an[IncorrectParamsException] should be thrownBy TransformationParamsParser().parseTransformationParams(incorrectJson)
  }
}
