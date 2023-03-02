import org.scalatest.FunSuite
import org.specs2.control.Functions.toStrictFunction1
import rever.etl.data_sync.jobs.call.CallHistoryNormalizer
import rever.etl.data_sync.jobs.call.CallHistoryNormalizer._
import rever.etl.data_sync.util.NormalizedPhoneUtils.normalizeNumberPhone
import rever.etl.data_sync.util._

import scala.language.postfixOps

class CallNumberNormalizeTest extends FunSuite {

  test("Normalize number phone") {

    //VN phone
    assertResult("+84778785452")(CallHistoryNormalizer.getNormalizedPhoneFromMap(normalizeNumberPhone("01228785452")))
    assertResult("+84357216792")(CallHistoryNormalizer.getNormalizedPhoneFromMap(normalizeNumberPhone("01657216792")))
    assertResult("+84708961204")(CallHistoryNormalizer.getNormalizedPhoneFromMap(normalizeNumberPhone("0708961204")))
    assertResult("+84358036082")(CallHistoryNormalizer.getNormalizedPhoneFromMap(normalizeNumberPhone("0358036082")))
    assertResult("+842835211690")(CallHistoryNormalizer.getNormalizedPhoneFromMap(normalizeNumberPhone("02835211690")))
    assertResult("")(CallHistoryNormalizer.getNormalizedPhoneFromMap(normalizeNumberPhone("+84821037272070")))
    assertResult("+84903008825")(CallHistoryNormalizer.getNormalizedPhoneFromMap(normalizeNumberPhone("Trang0903008825")))
    assertResult("+84914081070")(CallHistoryNormalizer.getNormalizedPhoneFromMap(normalizeNumberPhone("Chnga0914081070")))
    assertResult("+84364009784")(CallHistoryNormalizer.getNormalizedPhoneFromMap(normalizeNumberPhone("0364+009784")))
    assertResult("+84984678564")(CallHistoryNormalizer.getNormalizedPhoneFromMap(normalizeNumberPhone("tel+84984678+564")))

  }

  test("Normalize Service Phone") {
    //Service phone
    assertResult("11122")(CallHistoryNormalizer.getNormalizedPhoneFromMap(normalizeNumberPhone("11122")))
    assertResult("11121")(CallHistoryNormalizer.getNormalizedPhoneFromMap(normalizeNumberPhone("11121")))
    assertResult("12194")(CallHistoryNormalizer.getNormalizedPhoneFromMap(normalizeNumberPhone("12194")))
  }

  test("Normalize Foreign Phone") {
    //Foreign phone
    assertResult("+19163997992")(CallHistoryNormalizer.getNormalizedPhoneFromMap(normalizeNumberPhone("0019163997992")))
    assertResult("+4915214152993")(CallHistoryNormalizer.getNormalizedPhoneFromMap(normalizeNumberPhone("004915214152993")))
    assertResult("")(CallHistoryNormalizer.getNormalizedPhoneFromMap(normalizeNumberPhone("+85684935650685")))
    assertResult("")(CallHistoryNormalizer.getNormalizedPhoneFromMap(normalizeNumberPhone("+85684935650685")))
    assertResult("+18447676177")(CallHistoryNormalizer.getNormalizedPhoneFromMap(normalizeNumberPhone("+18447676177")))
  }

  test("Not a phone number") {
    assertResult("")(CallHistoryNormalizer.getNormalizedPhoneFromMap(normalizeNumberPhone("mcf_message3762")))
    assertResult("")(CallHistoryNormalizer.getNormalizedPhoneFromMap(normalizeNumberPhone("Đinh Bảo Duy")))
    assertResult("")(CallHistoryNormalizer.getNormalizedPhoneFromMap(normalizeNumberPhone("myrever_1584326092814000")))
    assertResult("")(CallHistoryNormalizer.getNormalizedPhoneFromMap(normalizeNumberPhone("Nguyễn Thị Tuyết Trinh")))
  }
}
