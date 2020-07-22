package com.esamusenko.task.handler

import com.esamusenko.task.SparkSessionSuite
import com.esamusenko.task.model.TransformationParams
import org.apache.spark.sql.types.{DateType, IntegerType, StringType}
import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.joda.time.DateTime
import org.scalatest.{FunSuite, Matchers}

class ColumnGeneratorSpec extends FunSuite with Matchers with SparkSessionSuite {

  import sparkSession.implicits._

  val data = Seq(Person("Petr", "1000", "26-01-1995"),
    Person("Yegor", "1200", "25-01-1995"),
    Person("Igor", "3000", "242-01-1995"),
    Person(null, "2000", "23-01-1995"),
    Person("Ivan", "aaa", "22-01-1995"))

  val df: DataFrame = sparkSession.sparkContext.parallelize(data).toDF()

  test("change name to nickname. Number of columns in new DF should be 1") {
    val param = TransformationParams("name", "nickname", StringType, None)

    val column = ColumnGenerator.generateNewColumn(param)

    val columns = df.select(column).columns

    columns.length shouldBe 1
  }

  test("change name to nickname. Name of columns in new DF should be nickname") {
    val param = TransformationParams("name", "nickname", StringType, None)

    val column = ColumnGenerator.generateNewColumn(param)

    val columns = df.select(column).columns

    columns.head shouldBe "nickname"
  }

  test("change salary to salary_int. Name of columns in new DF should be nickname") {
    val param = TransformationParams("salary", "salary_int", IntegerType, None)

    val column = ColumnGenerator.generateNewColumn(param)

    val columns = df.select(column).columns

    columns.head shouldBe "salary_int"
  }

  test("change salary to salary_int. Values which are not able to convert to int should be replaced null") {
    val param = List(TransformationParams("name", "name", StringType, None),
      TransformationParams("salary", "salary_int", IntegerType, None))

    val columns = ColumnGenerator.generateNewColumns(param)

    val persons = df.select(columns: _*).collect().map(row => {

      val salary = if (row.isNullAt(1)) None else Option(row.getInt(1))

      ConvertedPerson(row.getAs[String]("name"), salary, None)
    })

    val personWithNullSalary = persons.filter(_.salary.isEmpty)

    personWithNullSalary.length shouldBe 1

    personWithNullSalary.head.name shouldBe "Ivan"

  }

  test("change birthDate to d_o_b. Values which are not able to convert to int should be replaced null") {
    val param = List(TransformationParams("name", "name", StringType, None),
      TransformationParams("birthDate", "d_o_b", DateType, Option("dd-MM-yyyy")))

    val columns = ColumnGenerator.generateNewColumns(param)

    val persons = df.select(columns: _*).collect().map(row => ConvertedPerson(row.getAs[String]("name"), None, Option(row.getAs[DateTime]("d_o_b"))))

    val personWithNullBirthDate = persons.filter(_.birthDate.isEmpty)

    personWithNullBirthDate.length shouldBe 1

    personWithNullBirthDate.head.name shouldBe "Igor"

  }

  test("collect for unknown_column should throw AnalysisException") {
    val param = List(TransformationParams("unknown_column", "unknown_column", IntegerType, None))

    val columns = ColumnGenerator.generateNewColumns(param)

    an[AnalysisException] should be thrownBy df.select(columns: _*).collect()
  }

}


case class Person(name: String, salary: String, birthDate: String)

case class ConvertedPerson(name: String, salary: Option[Int], birthDate: Option[DateTime])