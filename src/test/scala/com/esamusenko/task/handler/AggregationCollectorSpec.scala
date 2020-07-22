package com.esamusenko.task.handler

import com.esamusenko.task.SparkSessionSuite
import com.esamusenko.task.model.TransformationParams
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.scalatest.{FunSuite, Matchers}

class AggregationCollectorSpec extends FunSuite with Matchers with SparkSessionSuite {

  import sparkSession.implicits._

  val columns = Seq("name", "salary")
  val data = Seq(("Petr", 1000), ("Yegor", 1200), ("Petr", 3000), (null, 2000))

  val df: DataFrame = sparkSession.sparkContext.parallelize(data).toDF(columns: _*)

  test("result for name should have result element with 2 values") {
    val param = TransformationParams("name", "name", StringType, None)

    val res = AggregationCollector.collect(df, param)

    res.column shouldBe "name"

    res.values.size shouldBe 2

    res.values shouldBe Map("Petr" -> 2, "Yegor" -> 1)
  }

  test("result for salary should have result element with 3 values") {
    val param = TransformationParams("salary", "salary", IntegerType, None)

    val res = AggregationCollector.collect(df, param)

    res.column shouldBe "salary"

    res.values.size shouldBe 4

    res.values shouldBe Map("1000" -> 1, "1200" -> 1, "3000" -> 1, "2000" -> 1)
  }

  test("result for name, salary should have 2 result elements") {
    val param = List(TransformationParams("salary", "salary", IntegerType, None),
      TransformationParams("name", "name", StringType, None))

    val res = AggregationCollector.collect(df, param)

    res.size shouldBe 2
  }

  test("collect for unknown_column should throw AnalysisException") {
    val param = List(TransformationParams("unknown_column", "unknown_column", IntegerType, None))

    an[AnalysisException] should be thrownBy AggregationCollector.collect(df, param)
  }

}
