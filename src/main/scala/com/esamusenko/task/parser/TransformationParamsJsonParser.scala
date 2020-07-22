package com.esamusenko.task.parser

import com.esamusenko.task.exception.IncorrectParamsException
import com.esamusenko.task.model.TransformationParams
import org.json4s.JsonAST.{JArray, JValue}
import org.json4s.jackson.JsonMethods

object TransformationParamsJsonParser extends TransformationParamsParser {
  def parseTransformationParams(input: String): List[TransformationParams] = {
    import org.json4s.DefaultFormats
    implicit val formats: DefaultFormats.type = DefaultFormats

    def getTransformationParams(jValue: JValue) = {
      TransformationParams((jValue \ "existing_col_name").extract[String],
        (jValue \ "new_col_name").extract[String],
        (jValue \ "new_data_type").extract[String],
        (jValue \ "date_expression").extractOpt[String])
    }

    JsonMethods.parse(input) match {
      case JArray(arr) => arr.map(getTransformationParams)
      case _ => throw new IncorrectParamsException("Incorrect expected JSON format")
    }
  }
}
