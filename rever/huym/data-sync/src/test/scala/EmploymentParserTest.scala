import com.fasterxml.jackson.databind.JsonNode
import org.scalatest.FunSuite
import rever.etl.data_sync.util.EmploymentStatusUtils
import vn.rever.common.util.JsonHelper

/** @author anhlt (andy)
  * @since 27/04/2022
  */
class EmploymentParserTest extends FunSuite {
  test("Convert employment parser from json") {

    val employmentStatusJsonStr =
      """
        |{"status":"official","comment":"add by system","effective_date":"14/08/2017"}
        |""".stripMargin

    val employmentStatus = EmploymentStatusUtils.fromJsonNode(JsonHelper.fromJson[JsonNode](employmentStatusJsonStr))

    println(employmentStatus)

    assertResult("official")(employmentStatus.status)
    assertResult("14/08/2017")(employmentStatus.effectiveDate)
    assertResult(1502643600000L)(employmentStatus.effectiveTime)
  }
}
