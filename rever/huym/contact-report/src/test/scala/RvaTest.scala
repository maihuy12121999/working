import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, countDistinct}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/** @author anhlt (andy)
  * @since 27/04/2022
  */
object RvaTest {
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

    val path = "/Users/andy/Desktop/part-00000-ac553795-ebcf-4598-a583-ead7f1df080b-c000.snappy (1).parquet"
    val df = sparkSession.read.parquet(path)

    df
      .where(col("user_type").equalTo("rva"))
      .where(col("status").equalTo("1"))
      .groupBy(
        col("user_type")
      )
      .agg(
        countDistinct(col("username"))
      )
      .show(5000, false)

  }
}
