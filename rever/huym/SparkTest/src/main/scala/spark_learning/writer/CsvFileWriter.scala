package spark_learning.writer
import org.apache.spark.sql.{Dataset, Row}
import rever.etl.rsparkflow.api.SinkWriter
import rever.etl.rsparkflow.api.configuration.Config
import org.apache.spark.sql.{DataFrame, SaveMode}
class CsvFileWriter extends SinkWriter {
  override def write(s: String, dataset: Dataset[Row], config: Config): Dataset[Row] = {
    val outputPath = config.get("output_path")
    val tablePath = s"${outputPath}/$s"
    dataset
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header",value = true)
      .csv(tablePath)
    dataset
  }
}
