import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import java.util

/** @author anhlt (andy)
  * @since 27/04/2022
  */
object LeftAntiJoinTest {
  def main(args: Array[String]): Unit = {

    for (i <- 5 to (10, 1)) {
      println(i)
    }

    val sparkSession = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()

    val structType = StructType(
      Array(
        StructField("channel", DataTypes.StringType, false),
        StructField("id", DataTypes.StringType, false),
        StructField("age", DataTypes.IntegerType, false),
        StructField("address", DataTypes.StringType, false),
        StructField("timestamp", DataTypes.LongType, false)
      )
    )

    val previousDf = sparkSession.createDataFrame(
      util.Arrays.asList(
        Row("web", "1000", 18, "HCM city", System.currentTimeMillis()),
        Row("web", "1002", 26, "HCM city", System.currentTimeMillis()),
        Row("web", "1004", 35, "HCM city", System.currentTimeMillis()),
        Row("mobile", "1005", 32, "HCM city", System.currentTimeMillis())
      ),
      structType
    )

    val dailyDf = sparkSession.createDataFrame(
      util.Arrays.asList(
        Row("web", "1002", 26, "HCM city", System.currentTimeMillis()),
        Row("mobile", "1003", 44, "HCM city", System.currentTimeMillis()),
        Row("web", "1004", 35, "HCM city", System.currentTimeMillis()),
        Row("web", "1006", 21, "HCM city", System.currentTimeMillis()),
        Row("desktop", "1003", 34, "HCM city", System.currentTimeMillis())
      ),
      structType
    )

    val resultDf = dailyDf.join(
      previousDf,
      dailyDf("channel") === previousDf("channel") &&
        dailyDf("id") === previousDf("id"),
      "left_anti"
    )

    resultDf.printSchema()
    resultDf.show(100)

    //The expected output should be
    """
      |+-------+----+---+--------+-------------+
      ||channel|  id|age| address|    timestamp|
      |+-------+----+---+--------+-------------+
      || mobile|1003| 44|HCM city|1652773305222|
      ||    web|1006| 21|HCM city|1652773305222|
      ||desktop|1003| 34|HCM city|1652773305222|
      |+-------+----+---+--------+-------------+""".stripMargin

  }
}
