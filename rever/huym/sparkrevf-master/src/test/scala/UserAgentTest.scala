import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{callUDF, col}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.FunSuite
import rever.rsparkflow.spark.api.udf.RUdfUtils
import rever.rsparkflow.spark.utils.JsonUtils
import ua_parser.Parser

/**
  * @author anhlt
  */
class UserAgentTest extends FunSuite {

  val parser = new Parser()

  test("Test parse user agent") {
    val userAgents = Seq(
      "SAMSUNG-SM-B313E Opera/9.80 (J2ME/MIDP; Opera Mini/4.5.40318/191.227; U; en) Presto/2.12.423 Version/12.16",
      "Opera/9.80 (J2ME/MIDP; Opera Mini/8.0.35626/182.104; U; vi) Presto/2.12.423 Version/12.16",
      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36",
      "Mozilla/5.0 (X11; U; SunOS sun4u; en-US; rv:1.8.1.11) Gecko/20080118 Firefox/50.0.1",
      "Mozilla/5.0 (X11; U; SunOS sun4u; en-US; rv:1.9b5) AppleWebKit/537.15 (KHTML, like Gecko) Chrome/97.0.4692.71 (Actually it's IE 7.0 but every site is a faggot that thinks it should be crippled for old browsers. Fuck you, webniggers.)",
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:100.0) Gecko/20100101 Firefox/100.0",
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36",
      "Mozilla/5.0 (Linux; Android 12; SM-S906N Build/QP1A.190711.020; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/80.0.3987.119 Mobile Safari/537.36",
      "Mozilla/5.0 (Mobile; U; Windows Phone 11; Android 11.0; ARM; Trident/43.0; Touch; rv:43.0; Edg/99.0.1150.55; NOKIA; 3.1 Plus) like iPhone OS 14_0_3 Mac OS X AppleWebKit/637 (KHTML, like Gecko) Mobile Safari/637",
      "Xbox/2.0.17559.0 UPnP/1.0 Xbox/2.0.17559.0",
      "Mozilla/5.0 (Mobile; Windows Phone 8.1; Android 5.0; ARM; Trident/7.0; Touch; rv:11.0; IEMobile/11.0; NOKIA; Lumia 3169)",
      "Mozilla/5.0 (Linux; Android 10; HarmonyOS; ELS-AN00; HMSCore 6.6.0.312) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.105 HuaweiBrowser/12.1.1.321 Mobile Safari/537.36",
      "Mozilla/5.0 (Windows; U; Windows CE; Mobile; like Android; ko-kr) AppleWebKit/533.3 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.3 custom",
      "Mozilla/5.0 (X11; Fuchsia) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.116 Safari/537.36 CrKey/1.56.500000",
      "PalmSource/Palm-D053; Blazer/4.5) 16;320x320 UP.Link/6.3.1.17.06.3.1.17.0",
      "Mozilla/5.0 (Linux; Android 5.0.1; Konka Android TV 828 Build/LRX22C) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/37.0.0.0 Safari/537.36"
    )

    val sparkSession = SparkSession.builder().master("local[2]").getOrCreate()
    RUdfUtils.registerAll(sparkSession)

    val schema = StructType(
      Array(
        StructField("user_agent", DataTypes.StringType, nullable = true)
      )
    )
    val df = sparkSession.createDataset(userAgents.map(Row(_)))(RowEncoder(schema))

    df.withColumn("client_info", callUDF(RUdfUtils.RV_PARSE_USER_AGENT, col("user_agent")))
      .select(
        col("user_agent"),
        col("client_info.*")
      )
      .show(1000)

    //
  }

  test("Parse User-Agent: on Windows") {
    val userAgent =
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:100.0) Gecko/20100101 Firefox/100.0"

    val clientInfo = parser.parse(userAgent)

    println(JsonUtils.toJson(clientInfo))

    assertResult("Firefox")(clientInfo.userAgent.family)
    assertResult("100")(clientInfo.userAgent.major)

    assertResult("Windows")(clientInfo.os.family)
    assertResult("10")(clientInfo.os.major)

    assertResult("Other")(clientInfo.device.family)
  }

  test("Parse User-Agent: Chrome on MAC") {
    val userAgent =
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36"

    val clientInfo = parser.parse(userAgent)

    println(JsonUtils.toJson(clientInfo))

    assertResult("Chrome")(clientInfo.userAgent.family)
    assertResult("101")(clientInfo.userAgent.major)

    assertResult("Mac OS X")(clientInfo.os.family)
    assertResult("10")(clientInfo.os.major)

    assertResult("Mac")(clientInfo.device.family)
  }

  test("Parse User-Agent: Chrome on Linux") {
    val userAgent =
      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36"

    val clientInfo = parser.parse(userAgent)

    println(JsonUtils.toJson(clientInfo))

    assertResult("Chrome")(clientInfo.userAgent.family)
    assertResult("97")(clientInfo.userAgent.major)

    assertResult("Linux")(clientInfo.os.family)
    assertResult(null)(clientInfo.os.major)

    assertResult("Other")(clientInfo.device.family)
  }

  test("Parse User-Agent: on Android") {
    val userAgent =
      "Mozilla/5.0 (Linux; Android 12; SM-S906N Build/QP1A.190711.020; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/80.0.3987.119 Mobile Safari/537.36"

    val clientInfo = parser.parse(userAgent)

    println(JsonUtils.toJson(clientInfo))

    assertResult("Chrome Mobile WebView")(clientInfo.userAgent.family)
    assertResult("80")(clientInfo.userAgent.major)

    assertResult("Android")(clientInfo.os.family)
    assertResult("12")(clientInfo.os.major)

    assertResult("Samsung SM-S906N")(clientInfo.device.family)
  }



}
