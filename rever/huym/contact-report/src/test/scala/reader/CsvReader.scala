package reader

import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config

class CsvReader extends SourceReader {
  val OLD_DATA_SCHEMA = StructType(
    Array(
      StructField("oppo_id", StringType),
      StructField("pipeline_id", IntegerType),
      StructField("phase_id", IntegerType),
      StructField("created_time", LongType),
      StructField("status", IntegerType),
      StructField("owner", StringType),
      StructField("owner_team", StringType),
      StructField("owner_mc", StringType),
      StructField("closed", BooleanType),
      StructField("won", BooleanType),
      StructField("lost", BooleanType)
    )
  )

  val DATA_SCHEMA = StructType(
    Array(
      StructField("oppo_id", StringType, true),
      StructField("pipeline_id", IntegerType, true),
      StructField("phase_id", IntegerType, true),
      StructField("created_time", LongType, true),
      StructField("status", IntegerType, true),
      StructField("owner", StringType, true),
      StructField("owner_team", StringType, true),
      StructField("owner_mc", StringType, true),
      StructField("closed", BooleanType, true),
      StructField("won", BooleanType, true),
      StructField("lost", BooleanType, true)
    )
  )

  override def read(tableName: String, config: Config): Dataset[Row] = {
    println(s"${getClass.getSimpleName}: $tableName at ${System.currentTimeMillis()}")
    val inputPath = config.get("input_path")
    val spark = SparkSession.active
    import spark.implicits._
    val df = spark.read
      .format("csv")
      .option("header", "true")
      .options(Map("delimiter" -> ",", "header" -> "true", "inferSchema" -> "true"))
      .csv(s"$inputPath/$tableName")
    val updateWithOldDataDf = df.withColumn("old_data", from_json($"old_data", OLD_DATA_SCHEMA))
    val updateWithNewDataDf = df.withColumn("data", from_json($"data", DATA_SCHEMA))
    for (columnName <- updateWithNewDataDf.select("data.*").columns) {
      println(columnName)
      if (!df.columns.contains(s"data.${columnName}")) {
        df.withColumn(s"data.$columnName", col(s"old_data.$columnName"))
      }
    }
    df
  }
}
