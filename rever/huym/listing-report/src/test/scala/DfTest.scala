import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, to_date, to_timestamp, udf, when}
import org.apache.spark.sql.types.{ArrayType, DataTypes, DoubleType, IntegerType, StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt
object DfTest {
  def main(args: Array[String]): Unit = {
    val data = Seq(26,
      26,
      26,
      29,
      30)

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()
    SparkSession.active.sparkContext.setLogLevel("ERROR")
    import spark.sqlContext.implicits._
    val appendListUdf = udf((nullColumn:Array[Long],newColumn:Array[Long])=>nullColumn++ newColumn)
    val simpleSchema = StructType(Array(
      StructField("value", DataTypes.createArrayType(StringType, true), true),
      StructField("tmp", StringType, true),
    ))
    val valueUdf = udf((tmp: Long, value: Array[Long]) =>
     {
        value match {
          case null =>
              Array.empty[Long] :+ tmp
          case _ =>
            value :+ tmp
        }
      }
    )
    val df = data.toDF("data")
    df.show()
    val mapDF = df.map(fun => {
      val l = Array(1, 2, 3)
      println((l :+ fun.getInt(0)).mkString("Array(", ", ", ")"))
      l :+ fun.getInt(0)
    })
      .withColumn("tmp", lit(1662347634444L))
      .withColumn("null_column", lit(null))
      .withColumn(
        "value",
        when(col("tmp").isNull,col("value"))
          .otherwise(valueUdf(col("tmp"), col("value")))
      )
      .withColumn("new_column", valueUdf(col("tmp"), lit(Array.empty[Long])))
      .withColumn("null_column", valueUdf(col("tmp"), col("null_column")))
      .withColumn("timestamp", lit(1662346836135L))
      .withColumn("null_column", valueUdf(col("timestamp"), col("null_column")))
      .withColumn("null_column",appendListUdf(col("null_column"),col("new_column")))

    mapDF.printSchema()
    mapDF.show(false)
    val optionArrayStr: Option[Array[String]] = Some(Array("skt", "aasdas"))
    optionArrayStr match {
      case None => print("None")
      case Some(array) => println((array :+ "geng").mkString("Array(", ", ", ")"))
    }
    //    mapDF.map(row=>{
    //      val map = row.getAs("value")
    //
    //    })
    val timestamp = "1662346836135"
    val array1 = Array(1,2,3)
    val array2 = Array(4,5,7)
    println((array1 ++ array2).mkString("Array(", ", ", ")"))
//    1656989532123, 1662345641078
//    1656990713320, 1662345615928
//    1656994867048, 1662347634444
  }

}
