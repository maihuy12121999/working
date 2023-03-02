package spark_learning
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config
import org.apache.spark.sql.SparkSession
import rever.etl.rsparkflow.RSparkFlow
import spark_learning.SQLPrograming.spark

import scala.collection.JavaConverters.mapAsJavaMapConverter


object ETLMain {
  def main(args: Array[String]): Unit = {
    val config = Config
      .builder()
      .addArgument("input_path",true)
      .addArgument("output_path",true)
      .addArgument("RV_JOB_ID",true)
      .addArgument("RV_EXECUTION_DATE",true)
      .build(
        Map[String,AnyRef](
          "input_path"->"input",
          "output_path"->"output",
          "RV_JOB_ID"->"spark_learning",
          "RV_EXECUTION_DATE"->"2022-06-28T00:00:00+00:00"
        ).asJava,
        true
      )
    val sparkSession =SparkSession
      .builder()
      .master("local[4]")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    import sparkSession.implicits._
    new RSparkFlow().run("spark_learning",config)
  }
}
