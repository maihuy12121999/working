import org.scalatest.{FunSuite, nocolor}
import vn.rever.common.util.PhoneUtils

/** @author anhlt (andy)
  * @since 27/04/2022
  */
class PhoneTest extends FunSuite {
  test("Normalize C3X Extension") {
    assertResult(None)(PhoneUtils.normalizePhone("1001"))
    assertResult(None)(PhoneUtils.normalizePhone("01227452341"))
    assertResult(None)(PhoneUtils.normalizePhone("+849454025137"))
  }
}
