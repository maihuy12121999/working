package spark_learning.flow
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.annotation.{Output, Table}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import spark_learning.SQLPrograming.df
import spark_learning.reader.CsvReader
import spark_learning.reader.JsonReader
import spark_learning.reader.TxtReader
import spark_learning.reader.ParquetReader
import spark_learning.writer.ParquetFileWriter
class DataSources extends FlowMixin {
  println("========================DataSources=====================")
  @Output(writer = classOf[ParquetFileWriter])
  @Table("nameAndFavorColor.parquet")
  def buildParquet(
           @Table(
             name = "users.parquet",
             reader = classOf[ParquetReader])
           peopleDF:DataFrame
           ):DataFrame ={
    println("========================Parquet=====================")
    peopleDF.select("name","favorite_color").show(10)
    peopleDF.select("name","favorite_color")
  }
  @Table("peopleCSV")
  def buildCsv(
                    @Table(
                      name = "people.csv",
                      reader = classOf[CsvReader])
                    peopleDF:DataFrame
                  ):DataFrame ={
    println("========================CSV=====================")
    peopleDF.show(10)
    peopleDF
  }
  @Table("peopleJSON")
  def buildJson(
                @Table(
                  name = "people.json",
                  reader = classOf[JsonReader])
                peopleDF:DataFrame
              ):DataFrame ={
    println("========================JSON=====================")
    peopleDF.show(10)
    peopleDF.printSchema()
    peopleDF
  }
}
