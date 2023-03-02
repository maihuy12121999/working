package cta_call_analysis.writer

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import rever.etl.rsparkflow.api.SinkWriter
import rever.etl.rsparkflow.api.configuration.Config

/**
 * @author anhlt (andy)
 * @since 14/07/2022
**/
 class TestUserEventWriter extends SinkWriter{
   override def write(tableName: String, tableData: Dataset[Row], config: Config): Dataset[Row] = {
     tableData.printSchema()
//     showDFbySourceType("Listing",tableData)
//     showDFbySourceType("Project",tableData)
//     showDFbySourceType("Profile",tableData)
     tableData
       .select("url","user_id","timestamp","alias","source_type","source_code","phone_number")
       .where(col("phone_number")=!="")
       .show(20)

     tableData
   }
  def showDFbySourceType(sourceType:String, dataFrame: DataFrame): Unit = {
    dataFrame.select("url","user_id","alias","source_type","source_code","phone_number").where(dataFrame("source_type")===sourceType).show(10,truncate = false)
  }
}
