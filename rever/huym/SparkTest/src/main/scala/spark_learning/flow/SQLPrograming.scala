package spark_learning.flow
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.annotation.Table
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import spark_learning.SQLPrograming.df
import spark_learning.reader.CsvReader
import spark_learning.reader.JsonReader
import spark_learning.reader.TxtReader
import org.apache.spark.sql.catalyst.plans._

case class Person(name:String, age:Long)
class SQLPrograming extends FlowMixin{
  @Table("people")
  def build(
             @Table(
               name="people.json",
               reader = classOf[JsonReader])
             peopleJson :DataFrame,
              @Table(
              name="people.txt",
              reader = classOf[TxtReader])
            peopleTxt :DataFrame): DataFrame ={
    println("========================SQLPrograming=====================")
    peopleJson.show(10)
    peopleJson.printSchema()
    peopleJson.select("name").show(10)
    peopleJson.createOrReplaceTempView("people")
    SparkSession.active.sql("select * from people").show()
    peopleTxt.show()
    peopleTxt
  }
}
