package com.esamusenko.task.parser

import com.esamusenko.task.model.TransformationParams

trait TransformationParamsParser {
  def parseTransformationParams(input: String): List[TransformationParams]
}

object TransformationParamsParser {
  def apply(): TransformationParamsParser = TransformationParamsJsonParser
}
