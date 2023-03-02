import org.scalatest.FunSuite
import rever.data_health.domain.listing.ListingScoreConfig
import rever.data_health.domain.p_contact.ContactScoreConfig
import vn.rever.common.util.JsonHelper

class ScoreConfigTest extends FunSuite {

  test("Personal Contact default score") {
    val config = ContactScoreConfig.default()

    println(JsonHelper.toJson(config, true))
  }

  test("Listing default score") {
    val config = ListingScoreConfig.default()

    println(JsonHelper.toJson(config, true))
  }
}
