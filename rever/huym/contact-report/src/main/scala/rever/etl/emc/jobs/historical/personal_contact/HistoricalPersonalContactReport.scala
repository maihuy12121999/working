package rever.etl.emc.jobs.historical.personal_contact

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import rever.etl.emc.domain.HistoricalPContactFields
import rever.etl.emc.jobs.historical.personal_contact.reader.HistoricalPersonalContactReader
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.annotation.Table
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.domain.SparkSessionImplicits._

class HistoricalPersonalContactReport extends FlowMixin {

  @Table("personal_contact_historical")
  def build(
      @Table(
        name = "historical.personal_contact_1",
        reader = classOf[HistoricalPersonalContactReader]
      ) dailyDf: DataFrame,
      config: Config
  ): DataFrame = {
    val previousA0Df = getPreviousA0DfFromS3(config.getJobId, config.getDailyReportTime, config)
    val a0Df = buildA0Df(previousA0Df, dailyDf)
    val a1Df = dailyDf.drop(HistoricalPContactFields.TIMESTAMP)
    exportA1DfToS3(config.getJobId, config.getDailyReportTime, a1Df, config)
    exportA0DfToS3(config.getJobId, config.getDailyReportTime, a0Df, config)
    a1Df
  }

  private def buildA0Df(previousA0Df: Option[DataFrame], dailyDf: DataFrame): DataFrame = {
    val resultDf = previousA0Df match {
      case None => dailyDf
      case Some(previousA0Df) =>
        previousA0Df
          .unionByName(dailyDf, allowMissingColumns = true)
          .dropDuplicateCols(
            Seq(HistoricalPContactFields.P_CID),
            col(HistoricalPContactFields.TIMESTAMP).desc
          )
    }
    resultDf.drop(HistoricalPContactFields.TIMESTAMP)
  }
}
