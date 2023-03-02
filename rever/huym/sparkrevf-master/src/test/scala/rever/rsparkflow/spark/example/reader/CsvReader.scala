package rever.rsparkflow.spark.example.reader

import org.apache.spark.sql.{DataFrame, SparkSession}
import rever.rsparkflow.spark.api.SourceReader
import rever.rsparkflow.spark.api.configuration.Config

class CsvReader extends SourceReader {

  def read(tableName: String, conf: Config): DataFrame = {
    println(
      s"${getClass.getSimpleName}: $tableName at ${System.currentTimeMillis()}"
    )

    val inputPath = conf.get("input_path")
    SparkSession.active.read
      .format("csv")
      .option("header", value = true)
      .options(
        Map("delimiter" -> ",", "header" -> "true", "inferSchema" -> "true")
      )
      .csv(s"$inputPath/$tableName")

  }
}
