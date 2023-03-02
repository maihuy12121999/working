import rever.etl.rsparkflow.utils.JsonUtils

import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.concurrent.atomic.LongAdder

case class NumericalValidation(from:Option[Double], to:Option[Double])
case class ListingMatchingStatsConfig(numericalValidation: Map[String, NumericalValidation], usdRate: Double)
object CaseClassJson {
  def main(args: Array[String]): Unit = {
    val json = "{\n   \"numerical_validation\":{\n        \"rent_price\":{\n           \"from\":0.0,\n           \"to\":1200000000.0\n         },\n         \"sale_price\":{\n           \"from\":0.0,\n           \"to\":50000000000.0\n         },\n         \"num_bath_room\":{\n           \"from\":0.0,\n           \"to\":12.0\n         },\n         \"num_bed_room\":{\n           \"from\":0.0,\n           \"to\":12.0\n         },\n         \"area\":{}\n   },\n   \"usd_rate\":23.585\n}"
    println(JsonUtils.toJsonNode(json).toPrettyString)
    val encode = Base64.getEncoder.encodeToString(json.getBytes(StandardCharsets.UTF_8))
    println(encode)
    val decode = new String(Base64.getDecoder.decode(encode))
    val listingMatchingStatsConfigClass = JsonUtils.fromJson[ListingMatchingStatsConfig](decode)
    val numericalValidationMap  = listingMatchingStatsConfigClass.numericalValidation
    val usdRate = listingMatchingStatsConfigClass.usdRate
    println(numericalValidationMap)
    println(usdRate)
  }
}
