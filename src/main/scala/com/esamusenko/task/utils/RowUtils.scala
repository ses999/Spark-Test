package com.esamusenko.task.utils

import com.esamusenko.task.utils.StringUtils.StringRich
import org.apache.spark.sql.Row

object RowUtils {

  implicit class RowRich(row: Row) {
    def existEmpty(): Boolean = {
      val len: Int = row.length
      var i = 0
      var existEmpty = false
      while (i < len && !existEmpty) {
        if (row.getString(i).isBlank) existEmpty = true
        i += 1
      }
      existEmpty
    }

    def nonExistEmpty(): Boolean = !existEmpty()
  }

}
