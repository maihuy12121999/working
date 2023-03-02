package rever.etl.support.chatbot_lac

import org.apache.spark.sql.DataFrame
import rever.rsparkflow.spark.FlowMixin
import rever.rsparkflow.spark.api.annotation.{Output, Table}
import rever.rsparkflow.spark.api.configuration.Config
import rever.etl.support.chatbot_lac.reader.LacRequestReader
import rever.etl.support.chatbot_lac.writer.LacToGoogleSheetWriter

class ExportLacRequest extends FlowMixin {

  @Output(writer = classOf[LacToGoogleSheetWriter])
  @Table("lac")
  def build(
      @Table(name = "lac_request", reader = classOf[LacRequestReader]) df: DataFrame,
      config: Config
  ): DataFrame = {

    df
  }

}
