import org.apache.spark.sql.SparkSession
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.utils.Utils
import com.fasterxml.jackson.databind.JsonNode
import rever.etl.rsparkflow.client.DataMappingClient
import rever.etl.rsparkflow.domain.User

import scala.collection.JavaConverters.mapAsJavaMapConverter

object test {
  def main(args: Array[String]): Unit = {
    val config = Config
      .builder()
      .addArgument("CH_DRIVER", true)
      .addArgument("CH_HOST", true)
      .addArgument("CH_PORT", true)
      .addArgument("CH_USER_NAME", true)
      .addArgument("CH_PASSWORD", true)
      .addArgument("RV_JOB_ID", true)
      .addArgument("RV_EXECUTION_DATE", true)
      .addArgument("RV_DATA_MAPPING_HOST", true)
      .addArgument("RV_DATA_MAPPING_USER", true)
      .addArgument("RV_DATA_MAPPING_PASSWORD", true)
      .addArgument("RV_RAP_INGESTION_HOST", true)
      .addArgument("RV_S3_ACCESS_KEY", true)
      .addArgument("RV_S3_SECRET_KEY", true)
      .addArgument("RV_S3_REGION", true)
      .addArgument("RV_S3_BUCKET", true)
      .addArgument("RV_S3_PARENT_PATH", true)
      .build(Utils.parseArgToMap(args).asJava, true)

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()
    SparkSession.active.sparkContext.setLogLevel("ERROR")
    val client = DataMappingClient.client(config)
    val userIds = Seq("1653390235296000","709919008292390947","1940115493331756309","gg-117746096150577749523")
    val userIdMapRaw = client.mGetUser(userIds)
    val userIdMap = getUserProfileFromOwnerId(userIds,config)
    println(userIdMap)
    println(userIdMapRaw(userIds.head).userNode)
  }
  def getUserProfileFromOwnerId(userIds: Seq[String], config: Config): Map[String, (String,String,String)] = {
    val client = DataMappingClient.client(config)
    val userIdMap = client.mGetUser(userIds)
    userIdMap.map(
      e=>
        e._1->
          (e._2.workEmail("unknown"),
           e._2.userNode.at("/properties/job_title").asText("unknown"),
           e._2.teamId("")
          )
    )
  }
}
