package rever.etl.emc.utils

import org.apache.spark.sql.functions.udf

object ContactUtils {
  final val normalizeStrUdf = udf[String, String](normalizeStr)

  def normalizeStr(department: String): String = {
    department.trim.toLowerCase
      .replaceAll("\\s+", "_")
      .replaceAll("\\\\&+", "_")
      .replaceAll("\\W+", "_")
      .replaceAll("_+", "_")
  }

}
