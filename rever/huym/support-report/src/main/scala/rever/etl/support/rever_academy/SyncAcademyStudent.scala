package rever.etl.support.rever_academy

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import rever.rsparkflow.spark.FlowMixin
import rever.rsparkflow.spark.api.annotation.{Output, Table}
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.client.DataMappingClient
import rever.rsparkflow.spark.domain.user.User
import rever.etl.support.rever_academy.reader.AcademyStudentReader
import rever.etl.support.rever_academy.writer.AcademyStudentWriter

class SyncAcademyStudent extends FlowMixin {
  @Output(writer = classOf[AcademyStudentWriter])
  @Table("academy_students")
  def build(
      @Table(name = "course_student", reader = classOf[AcademyStudentReader]) df: DataFrame,
      config: Config
  ): DataFrame = {
    val exportSchema = StructType(
      Array(
        StructField("course", DataTypes.StringType, nullable = false),
        StructField("username", DataTypes.StringType, nullable = false),
        StructField("email", DataTypes.StringType, nullable = false),
        StructField("name", DataTypes.StringType, nullable = false)
      )
    )

    val resultDf = df
      .mapPartitions(rows => enhanceUserInfo(rows.toSeq, exportSchema, config))(RowEncoder(exportSchema))
      .select(
        col("course"),
        col("username"),
        col("email"),
        col("name")
      )

    resultDf
  }

  private def enhanceUserInfo(rows: Seq[Row], schema: StructType, config: Config): Iterator[Row] = {
    val emails = rows
      .map(row => row.getAs[String]("email"))
      .filter(_ != null)
      .filter(_.nonEmpty)

    val usernameMap = mGetUsernameByEmail(config, emails)

    rows.map { row =>
      val course = row.getAs[String]("course")
      val email = row.getAs[String]("email")
      val name = row.getAs[String]("name")
      val username = usernameMap.get(email) match {
        case Some(user) => user.userNode.at("/username").asText()
        case None       => ""
      }

      new GenericRowWithSchema(
        Array(course, username, email, name),
        schema
      )
    }.toIterator
  }

  private def mGetUsernameByEmail(config: Config, emails: Seq[String]): Map[String, User] = {
    val client = DataMappingClient.client(config)
    client.mGetUserByEmail(emails)
  }
}
