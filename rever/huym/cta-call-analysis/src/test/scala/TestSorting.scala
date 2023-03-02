import org.apache.spark.sql.{Row, SparkSession}

import scala.math.Ordering.comparatorToOrdering

object TestSorting {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
//    Array([+84787237238,12150,12150,canceled,0,1656064091000,1656064091000],
    //    [+84915789776,11121,11121,connected,46000,1656064151000,1656064197000],
    //    [+84769915617,11132,11132,connected,89000,1656064546000,1656064635000])
    val phoneNumber = "+84787237238"
    val df : Array[Row] = Array(Row("+84787237238","12150","12150","canceled",4,50,1656064091000L),
                               Row("+84787237238","12150","12150","canceled",10,30,1656064091000L),
                              Row("+84915789776","11121","11121","connected",9,40,1656064197000L),
                              Row("+84769915617","11132","11132","connected",5,20,1656064635000L))
    println(df.sortBy(row => (row.getString(0) != phoneNumber,row.getInt(5))).mkString("Array(", ", ", ")"))

  }
}

