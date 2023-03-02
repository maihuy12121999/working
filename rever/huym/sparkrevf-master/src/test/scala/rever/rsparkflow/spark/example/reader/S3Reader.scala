package rever.rsparkflow.spark.example.reader

import org.apache.spark.sql.{DataFrame, SparkSession}
import rever.rsparkflow.spark.api.SourceReader
import rever.rsparkflow.spark.api.configuration.Config

class S3Reader extends SourceReader {

  def read(tableName: String, conf: Config): DataFrame = {

    println(
      s"${getClass.getSimpleName}: $tableName at ${System.currentTimeMillis()}"
    )

    val inputPath = conf.get("input_path")
    SparkSession.active.read.parquet(s"$inputPath/$tableName")

  }
}
