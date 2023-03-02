package rever.etl.support.export_inquiry

import org.apache.spark.sql.DataFrame
import rever.rsparkflow.spark.FlowMixin
import rever.rsparkflow.spark.api.annotation.Table
import rever.rsparkflow.spark.api.configuration.Config

class InquiryDescription extends FlowMixin{

  @Table("inquiry_description")
  def build(
      @Table(
        name = "inquiry_datamart",
        reader = classOf[InquiryDescriptionReader]
      ) inquiryDf: DataFrame,
      config: Config
  ): DataFrame = {

    exportA1DfToS3(config.getJobId, System.currentTimeMillis(), inquiryDf, config, Some("inquiry_description"))
    inquiryDf
  }
}
