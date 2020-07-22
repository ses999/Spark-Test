package com.esamusenko.task.utils

object StringUtils {

  implicit class StringRich(s: String) {

    def isBlank: Boolean = {
      if (s == null) false
      else {
        val newStr = s.trim
        if (newStr.length == 0) true
        else {
          val len = newStr.length
          var i = 1
          var isBlank = true
          while (i < len - 1 && isBlank) {
            val value = newStr(i)
            if (!Character.isWhitespace(value)) isBlank = false
            i += 1
          }
          isBlank && newStr.head == '‘' && newStr.last == '’'
        }
      }
    }

  }

}
