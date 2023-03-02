package rever.etl.emc.tool.hot_lead_export.reader.test

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.domain.SparkSessionImplicits._
import rever.etl.emc.tool.hot_lead_export.domain.HotLeadFields

class CsvHistoricalPersonalContactReader extends SourceReader{
  override def read(tableName: String, config: Config): Dataset[Row] = {
    SparkSession.active
      .read
      .format("csv")
      .option("header","true")
      .load("data/historical_personal_contact.csv")
      .drop("user_tags")
      .withColumn(HotLeadFields.TIMESTAMP,col(HotLeadFields.TIMESTAMP).cast(LongType))
      .dropDuplicateCols(
        Seq(HotLeadFields.P_CID, HotLeadFields.CID, HotLeadFields.OWNER_ID),
        col(HotLeadFields.TIMESTAMP).asc
      )
  }
}
