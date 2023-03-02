package spark_learning.reader

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config
class JsonReader extends SourceReader{
  override def read(s: String, config: Config): Dataset[Row] ={
    println(s"${getClass.getSimpleName}:$s")
    val inputPath = config.get("input_path")
    SparkSession.active.read
      .format("json")
      .json(s"$inputPath/$s")
  }
}
