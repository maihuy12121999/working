package writer

import org.apache.spark.sql.DataFrame
import rever.etl.rsparkflow.api.SinkWriter
import rever.etl.rsparkflow.api.configuration.Config

class SimpleWriter extends SinkWriter {
  override def write(
      tableName: String,
      dataFrame: DataFrame,
      config: Config
  ): DataFrame = {
    dataFrame.show(10)
    println(
      s"${getClass.getSimpleName}: $tableName at  ${System.currentTimeMillis()}"
    )
    dataFrame
  }
}
