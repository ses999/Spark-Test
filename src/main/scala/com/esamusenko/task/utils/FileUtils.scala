package com.esamusenko.task
package utils

import java.io.File

import scala.io.Source

object FileUtils {
  def getStringFromResource(path: String): String = using(Source.fromFile(new File(getClass.getResource(path).getPath)))(_.mkString)
}
