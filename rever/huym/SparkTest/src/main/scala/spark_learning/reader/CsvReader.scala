package spark_learning.reader

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config

class CsvReader extends SourceReader{
  override def read(s: String, config: Config): Dataset[Row] = {
    println(s"${getClass.getSimpleName}:$s")
    val inputPath = config.get("input_path")
    SparkSession.active.read
      .format("csv")
      .option("header", value = true)
      .options(Map("delimiter"->";", "header"->"true"))
      .csv(s"$inputPath/$s")
  }
}
