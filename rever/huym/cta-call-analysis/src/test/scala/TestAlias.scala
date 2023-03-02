import org.apache.spark.sql.SparkSession
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.client.DataMappingClient

import java.net.URL
import java.text.SimpleDateFormat
import scala.language.postfixOps
import scala.collection.JavaConverters.mapAsJavaMapConverter
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
object TestAlias{
  def main(args: Array[String]): Unit = {
    //rever.vn/mua/[alias]
    //rever.vn/mua/[alias]?
    //rever.vn/mua/[alias]#
    //rever.vn/du-an/[alias]
    //rever.vn/du-an/[alias]?
    //rever.vn/du-an/[alias]#
    //rever.vn/chuyen-vien/[alias]
    //rever.vn/chuyen-vien/[alias]?
    //rever.vn/chuyen-vien/[alias]#

    val config = Config
      .builder()
      .addArgument("CH_DRIVER",true)
      .addArgument("CH_HOST",true)
      .addArgument("CH_PORT",true)
      .addArgument("CH_USER_NAME",true)
      .addArgument("CH_PASSWORD",true)
      .addArgument("RV_EXECUTION_DATE",true)
      .addArgument("RV_DATA_MAPPING_HOST",true)
      .addArgument("RV_DATA_MAPPING_USER",true)
      .addArgument("RV_DATA_MAPPING_PASSWORD",true)
      .build(
        Map[String,AnyRef](
          "CH_DRIVER"->"ru.yandex.clickhouse.ClickHouseDriver",
          "CH_HOST"->"172.31.24.169",
          "CH_PORT"->"8123",
          "CH_USER_NAME"->"r3v3r_etl_devteam",
          "CH_PASSWORD"->"AEEFEW34r3v3r12ervrSWSQHHI=",
          "RV_JOB_ID"->"cta-call-analysis",
          "RV_EXECUTION_DATE"->"2022-02-04T00:00:00+00:00",
          "RV_DATA_MAPPING_HOST"->"http://eks-svc.reverland.com:31462",
          "RV_DATA_MAPPING_USER"->"rever",
          "RV_DATA_MAPPING_PASSWORD"->"rever@123!"
        ).asJava, true
      )
    val sparkSession  = SparkSession
      .builder()
      .master("local[4]")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
//    import sparkSession.implicits._
    val url = "abc"
    val alias = getAliasFromUrl(url)
    println(alias)
//    val id = getIdFromAlias(alias,config)
//    println(id)
//    val deathTime = "2019-03-14 01:22:45"
//    val deathDate = LocalDateTime.parse(deathTime, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
//    println(deathDate.minusHours(5))
//    println(config.getDailyReportTime)
//    val minusHourDurationUdf = udf((dateTime:String)=>{
//      val dateTimeConverted = LocalDateTime
//        .parse(dateTime, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
//      dateTimeConverted.minusHours(8).toString
//    })


//    val x =  List(0,1,2)
//    println(x.map(x =>List(x,x+1,x+2)))
//    println(x.flatMap(x=>List(x,x+1,x+2)))
//    println(x.map(x =>(x,x+1,x+2)))

  }
  def getAliasFromUrl(url:String): String={
    val sourceTypeSet = Set("mua", "du-an", "chuyen-vien")
    try {
      url match {
        case null | "" => ""
        case _ =>
          val myUrl = new URL(url)
          val path = myUrl.getPath
          val pathNames = path.split("/")

          if (myUrl.getHost.endsWith("rever.vn") & sourceTypeSet.contains(pathNames(1)))
            pathNames(2).trim
          else ""
      }
    }
    catch{
      case e: Exception => ""
    }
  }
  def getIdFromAlias(alias: String, config: Config): String= {
    val client = DataMappingClient.client(config)
    val mapId=client.mGetIdFromAlias(Seq(alias))
    mapId.getOrElse(alias,"None")
  }
}
