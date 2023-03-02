import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructType}
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.client.DataMappingClient
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind._
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.Breaks.{break, breakable}
object TestGetNumberPhoneFromUser {
  def main(args: Array[String]): Unit = {
    val config = Config
      .builder()
      .addArgument("CH_DRIVER", true)
      .addArgument("CH_HOST", true)
      .addArgument("CH_PORT", true)
      .addArgument("CH_USER_NAME", true)
      .addArgument("CH_PASSWORD", true)
      .addArgument("RV_EXECUTION_DATE", true)
      .addArgument("RV_DATA_MAPPING_HOST", true)
      .addArgument("RV_DATA_MAPPING_USER", true)
      .addArgument("RV_DATA_MAPPING_PASSWORD", true)
      .addArgument("CTA_EVENTS", true)
      .build(
        Map[String, AnyRef](
          "CH_DRIVER" -> "ru.yandex.clickhouse.ClickHouseDriver",
          "CH_HOST" -> "172.31.24.169",
          "CH_PORT" -> "8123",
          "CH_USER_NAME" -> "r3v3r_etl_devteam",
          "CH_PASSWORD" -> "AEEFEW34r3v3r12ervrSWSQHHI=",
          "RV_JOB_ID" -> "cta-call-analysis",
          "RV_EXECUTION_DATE" -> "2022-07-07T00:00:00+00:00",
          "RV_DATA_MAPPING_HOST" -> "http://eks-svc.reverland.com:31462",
          "RV_DATA_MAPPING_USER" -> "rever",
          "RV_DATA_MAPPING_PASSWORD" -> "rever@123!",
          "CTA_EVENTS" -> "AgentDetailV2_ClickAgentPhone,ProjectDetailV3_ClickRVA3CX,PropertyDetail_ClickRVA3CX"
        ).asJava, true
      )
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val userId = Seq("gg-1632819834897000_709","fb-1226173844117726","pn-1648966811455000_092","fb-1312566552088450")
//    up-32aba4ce-84f3-4dda-ae4d-df10df4319c6
//    gg-1644817663775000_220
//    gg-1648003466262000_052
    val userIdToPhoneNumberMap = getNumberPhoneFromUserId(userId, config)
    val phoneNumber= userIdToPhoneNumberMap.getOrElse(userId.head,"")
    println(userIdToPhoneNumberMap)
    println(phoneNumber)
//    val s = Seq(1,2,3)
//    for (i<-s.indices){
//      println(s(i))
//    }
//    84394225254
    //    val data2 = Seq(
    //      Row(Row("James","","Smith"),"OH","M",15),
    //      Row(Row("Anna","Rose",""),"NY","F",17),
    //      Row(Row("Julia","","Williams"),"OH","F",16),
    //      Row(Row("Maria","Anne","Jones"),"NY","M",4),
    //      Row(Row("Jen","Mary","Brown"),"NY","M",10),
    //      Row(Row("Mike","Mary","Williams"),"OH","M",20)
    //    )
    //    val schema = new StructType()
    //      .add("name",new StructType()
    //        .add("firstname",StringType)
    //        .add("middlename",StringType)
    //        .add("lastname",StringType))
    //      .add("state",StringType)
    //      .add("gender",StringType)
    //      .add("age",IntegerType)
    //    val df2 = spark.createDataFrame(
    //      spark.sparkContext.parallelize(data2),schema)
    //    df2.printSchema()
    //    df2.show(false)
    //    df2.sort("Age").show()
    //
    //    val arrayStructureData = Seq(
    //      Row(Row("James","","Smith"),List("Cricket","Movies"),Map("hair"->"black","eye"->"brown")),
    //      Row(Row("Michael","Rose",""),List("Tennis"),Map("hair"->"brown","eye"->"black")),
    //      Row(Row("Robert","","Williams"),List("Cooking","Football"),Map("hair"->"red","eye"->"gray")),
    //      Row(Row("Maria","Anne","Jones"),null,Map("hair"->"blond","eye"->"red")),
    //      Row(Row("Jen","Mary","Brown"),List("Blogging"),Map("white"->"black","eye"->"black"))
    //    )
    //    val arrayStructureSchema = new StructType()
    //      .add("name",new StructType()
    //        .add("firstname",StringType)
    //        .add("middlename",StringType)
    //        .add("lastname",StringType))
    //      .add("hobbies", ArrayType(StringType))
    //      .add("properties", MapType(StringType,StringType))
    //
    //    val df5 = spark.createDataFrame(
    //      spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
    //    val br = spark.sparkContext.broadcast(df5.collect())
    //    df5.printSchema()
    //    val resultSchema  = new StructType()
    //      .add("name",new StructType()
    //        .add("firstname",StringType)
    //        .add("middlename",StringType)
    //        .add("lastname",StringType))
    //      .add("state",StringType)
    //      .add("gender",StringType)
    //      .add("age",IntegerType)
    //      .add("new_column",IntegerType)
    //    df5.show()
    //    df5.foreach(row=>{
    //      val hobbies = row.getAs[Seq[String]]("hobbies")
    //      println(row)
    //      println(hobbies)
    //      val hairColor = row.getMap(2)
    //      Array("123","12321")
    //    })
    //    val resultDF = df2.map(row=>{
    //      val i = testUDF(br)
    //      Row(row.getStruct(0),row.getString(1),row.getString(2),row.getInt(3),i)
    //    })(RowEncoder(resultSchema))
    //    resultDF.show()
    //  }
    //  def testUDF(br :Broadcast[Array[Row]]):Int={
    //    var s = 0
    //    br.value.foreach(row=>{
    //      val hobbies = "happy"
    //      val hairColor = row.getMap(2)
    //      s +=1
    //    })
    //    s
    //  }
  }
  def extractPhoneNumberFromUserProfile(userProfile : JsonNode):String = {
    userProfile.at("/phone_number").asText()
  }
  def getNumberPhoneFromUserId(userIds: Seq[String], config: Config): Map[String, String] = {
    val client = DataMappingClient.client(config)
    val userIdMap = client.mGetFrontendUserProfiles(userIds)
    val phoneNumbers:ListBuffer[String] = ListBuffer.empty
    userIds.foreach{
      userId=>
        val userProfile= userIdMap.get(userId)
        val phoneNumber = userProfile match {
          case Some(x) => extractPhoneNumberFromUserProfile(x)
          case None => ""
        }
        phoneNumbers.append(phoneNumber)
    }
    var userIdToPhoneNumberMap : Map[String,String]= Map()
    for(i<- userIds.indices){
      userIdToPhoneNumberMap+=(userIds(i)->phoneNumbers(i))
    }
    userIdToPhoneNumberMap
  }
}
