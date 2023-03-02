package rever.rsparkflow.spark.example.flow

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.sum
import org.apache.spark.storage.StorageLevel
import rever.rsparkflow.spark.api.annotation.{Output, Table}
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.example.reader.CsvReader
import rever.rsparkflow.spark.example.writer.{ParquetFileWriter, SimpleWriter}

class ListingHHXBEnhance {}

class ListingHHXB {

  @Output(writer = classOf[SimpleWriter])
//  @Output(writer = classOf[ParquetFileWriter])
  @Table("listing_output")
  def build(
      @Table(
        name = "listing_hh_xb",
        reader = classOf[CsvReader]
      ) listingRaw: DataFrame,
      @Table(
        name = "listing",
        reader = classOf[CsvReader]
      ) listingPublishing: DataFrame,
      config: Config
  ): DataFrame = {

    listingPublishing.createOrReplaceTempView("listing_publishing")

    val df1 = listingPublishing.sqlContext.sql(
      """
        |SELECT team_id, project_id, rv_agent, first_published,republish_with_new_owner,republish_after_30day, 0 as  expired_to_published
        |FROM listing_publishing
        |""".stripMargin
    )

    listingRaw.createOrReplaceTempView("listing_raw")
    val df2 = listingRaw.sqlContext.sql(
      """
        |SELECT team_id, project_id, rv_agent,
        | 0 as first_published,
        | 0 as republish_with_new_owner,
        | 0 as republish_after_30day,
        | COUNT(DISTINCT property_id) as expired_to_published 
        |FROM listing_raw
        |GROUP BY team_id, project_id, rv_agent
        |ORDER BY team_id, project_id, rv_agent
        |""".stripMargin
    )

    val resultDf = df1
      .unionAll(df2)
      .groupBy("team_id", "project_id", "rv_agent")
      .agg(
        sum("first_published").name("first_published"),
        sum("republish_with_new_owner").name("republish_with_new_owner"),
        sum("republish_after_30day").name("republish_after_30day"),
        sum("expired_to_published").name("expired_to_published")
      )
      .persist(StorageLevel.MEMORY_AND_DISK_2)

    println("run flow")

    resultDf
  }
}
