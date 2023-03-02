package spark_learning.reader
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config
class ParquetReader extends SourceReader {
  override def read(s: String, config: Config): Dataset[Row] = {
    println(s"${getClass.getSimpleName}:$s")
    val inputPath = config.get("input_path")
    SparkSession.active.read
      .format("parquet")
      .parquet(s"$inputPath/$s")
  }
}
