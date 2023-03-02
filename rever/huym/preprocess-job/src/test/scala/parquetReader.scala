import org.apache.parquet.format.IntType
import org.apache.spark.sql.{Row, SparkSession, functions}
import org.apache.spark.sql.functions.{array, avg, col, collect_list, collect_set, mean, min, sort_array, udf}
import org.apache.spark.sql.types.{ArrayType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType}

object parquetReader {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val data = Seq(
      Row("architectural_style",0.0,0.0,0.0,List("-1","0","1","2","3")),
      Row("area",0.0,1000.0,0.0,List("-1","0","1","2","3")),
      Row("furniture_status",0.0,0.0,0.0,List("Basic","Empty","Full","unknown"))
    )
    val rawData = Seq(
      Row("furniture",4.0),
      Row("furniture",4.0),
      Row("furniture",1.0),
      Row("furniture",2.0),
      Row("furniture",3.0)
    )
    val furnitureDf = spark.createDataFrame(spark.sparkContext.parallelize(rawData),StructType(
      Seq(
        StructField("field_name",StringType,false),
        StructField("value",DoubleType,false)
      )
    ))
    val schema = StructType(
      Array(
        StructField("field",StringType,false),
        StructField("min_value",DoubleType,false),
        StructField("max_value",DoubleType,false),
        StructField("mean_value",DoubleType,false),
        StructField("unique_values",ArrayType(StringType,false),false)
      )
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
    furnitureDf.show()
    val uniqueValuesDf = furnitureDf
      .agg(
        min("value").as("min_value"),
        functions.max("value").as("max_value"),
        avg("value").as("avg_value"),
        array("value")
      )
//      .withColumn("unique_values",array("value"))
    uniqueValuesDf.printSchema()
    val a = "23.667"
    println(a.toDouble+1)
  }
}
