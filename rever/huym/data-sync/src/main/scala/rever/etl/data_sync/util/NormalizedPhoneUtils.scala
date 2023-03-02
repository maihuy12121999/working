package rever.etl.data_sync.util

import com.google.i18n.phonenumbers.PhoneNumberUtil
import org.apache.spark.sql.execution.datasources.json.JsonUtils
import rever.etl.data_sync.util.JsonUtils.toJson
import vn.rever.common.util.PhoneUtils

import java.util.regex.Pattern
import scala.collection.mutable
import scala.jdk.CollectionConverters.asScalaSetConverter
import scala.util.matching.Regex

object NormalizedPhoneUtils {
  private val phoneUtil:PhoneNumberUtil = PhoneNumberUtil.getInstance()
  private final val serviceDigitPattern = new Regex("^\\d{3,5}$")
  private final val pPattern = new Regex("(\\+?\\d{7,15})")
  final val priorityRegionCodes = Seq("VN","US","KR","JP","CN","HK","ID","IN","TH","SG","PH","MY","CA","FR","AU","CH","DE","ES","IT")
  final val nonPriorityRegionCodes = phoneUtil.getSupportedRegions.asScala.toSeq.filterNot(priorityRegionCodes.contains(_))


  private final val prefixPhoneMapping = Map(
    "+844" -> "+8424",
    "+848" -> "+8428",
    "+8461" -> "+84251",
    "+8462" -> "+84252",
    "+8464" -> "+84254",
    "+8455" -> "+84255",
    "+8456" -> "+84256",
    "+8457" -> "+84257",
    "+8458" -> "+84258",
    "+8468" -> "+84259",
    "+8460" -> "+84260",
    "+84501" -> "+84261",
    "+84500" -> "+84262",
    "+8463" -> "+84263",
    "+8459" -> "+84269",
    "+8470" -> "+84270",
    "+84651" -> "+84271",
    "+8472" -> "+84272",
    "+8473" -> "+84273",
    "+84650" -> "+84274",
    "+8475" -> "+84275",
    "+8466" -> "+84276",
    "+8467" -> "+84277",
    "+8422" -> "+84212",
    "+84231" -> "+84213",
    "+8420" -> "+84214",
    "+84230" -> "+84215",
    "+84229" -> "+84216",
    "+8452" -> "+84232",
    "+8453" -> "+84233",
    "+8454" -> "+84234",
    "+84510" -> "+84235",
    "+84511" -> "+84236",
    "+8437" -> "+84237",
    "+8438" -> "+84238",
    "+8439" -> "+84239",
    "+8433" -> "+84203",
    "+84240" -> "+84204",
    "+8425" -> "+84205",
    "+8426" -> "+84206",
    "+8427" -> "+84207",
    "+84280" -> "+84208",
    "+84281" -> "+84209",
    "+84320" -> "+84220",
    "+84321" -> "+84221",
    "+84241" -> "+84222",
    "+8431" -> "+84225",
    "+84351" -> "+84226",
    "+8436" -> "+84227",
    "+84350" -> "+84228",
    "+8430" -> "+84229",
    "+84780" -> "+84290",
    "+84781" -> "+84291",
    "+84710" -> "+84292",
    "+84711" -> "+84293",
    "+8474" -> "+84294",
    "+8476" -> "+84296",
    "+8477" -> "+84297",
    "+8479" -> "+84299"
  )

  def normalizedPhonesAllRegionMap(phone:String):mutable.Map[String,String]={
    val normalizedPhonesMap= mutable.Map.empty[String,String]
    phoneUtil.getSupportedRegions.forEach(
      region => {
        val normalizedPhone = PhoneUtils.normalizePhone(phone, region).getOrElse("")
        if(normalizedPhone!="") {
          normalizedPhonesMap.put(region,normalizedPhone)
        }
      }
    )
    normalizedPhonesMap
  }
  def normalizeNumberPhone(phoneNumber: String): mutable.Map[String,String] = {
    val removedSpacePhone = phoneNumber.replaceAll("\\s+", "")
    val firstExtractedPhone = pPattern.findFirstIn(
      removedSpacePhone match {
        case _ if(removedSpacePhone.startsWith("+")) => s"+${removedSpacePhone.replace("+", "")}"
        case _ => removedSpacePhone.replace("+", "")
      }
    )
    val extractedPhone = firstExtractedPhone match {
      case Some(extractedPhone)                   => extractedPhone
      case None                                  => removedSpacePhone
    }
    val convertedPhone = Seq(
      extractedPhone.startsWith("0"),
      pPattern.findFirstIn(extractedPhone).isDefined,
      extractedPhone.length == 10 || extractedPhone.length == 11
    ).forall(x => x) match {
      case true => s"+84${extractedPhone.drop(1)}"
      case _    => extractedPhone
    }

    if (convertedPhone.startsWith("+84") || convertedPhone.startsWith("84")) {
      val phone = convertedPhone.replaceFirst("^84", "+84")
      val normalizedPhone = PhoneUtils
        .normalizePhone(phone)
        .getOrElse(
          PhoneUtils.normalizePhone(convertPrefixPhoneNumber(phone)).getOrElse("")
        )
      normalizedPhone match {
        case "" =>normalizedPhonesAllRegionMap(extractedPhone)
        case normalizedPhone => mutable.Map("VN"->normalizedPhone)
      }
    } else if (pPattern.findFirstIn(convertedPhone).isDefined) {
      normalizedPhonesAllRegionMap(convertedPhone)
    } else if (convertedPhone.matches(serviceDigitPattern.regex)) {
      mutable.Map("VN"->convertedPhone)
    } else {
      mutable.Map.empty[String,String]
    }
  }

  private def convertPrefixPhoneNumber(rawPhoneNumber: String): String = {
    prefixPhoneMapping
      .find { case (oldOperator, _) =>
        rawPhoneNumber.startsWith(oldOperator)
      }
      .map { case (oldOperator, newOperator) =>
        rawPhoneNumber.replaceFirst(s"^${Pattern.quote(oldOperator)}", newOperator)
      }
      .getOrElse(rawPhoneNumber)
  }
}
