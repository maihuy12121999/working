package rever.etl.data_sync.util

object FinanceGSheetUtils {
  private final val serviceTypeMapping = Map(
    "bán" -> "sale",
    "thuê" -> "rent"
  )

  private final val levelMapping = Map(
    "sd" -> "sd",
    "sm" -> "sm",
    "sm" -> "sm",
    "rva s1" -> "rva_s1",
    "rva s2" -> "rva_s2",
    "f2" -> "f2",
    "pa" -> "pa",
    "bán chéo" -> "cross_sale",
    "hot bonus" -> "hot_bonus",
    "rva 1 side" -> "rva_1_side",
    "rva 2 side" -> "rva_2_side"
  )

  def getServiceType(name: String): String = {
    serviceTypeMapping.getOrElse(name.trim.toLowerCase(), "")
  }

  def getLevel(name: String): String = {
    levelMapping.getOrElse(name.trim.toLowerCase(), "")
  }

  def asStr(value: AnyRef): String = {
    value match {
      case value: String => value
      case value         => value.toString
    }
  }

  def asPercentInDouble(value: AnyRef): Double = {
    value match {
      case null                                        => 0.0
      case value: BigDecimal                           => value.toDouble
      case value: Number                               => value.doubleValue()
      case value if value.equals("-")                  => 0.0
      case value if value.toString.contains("#DIV/0!") => 0.0
      case value: String if value.trim.endsWith("%") =>
        value.trim.replaceAll("%", "").trim.toDouble / 100
      case value if value.toString.trim.isEmpty => 0.0
      case value                                => value.toString.toDouble
    }
  }

  def asDouble(value: AnyRef): Double = {
    try {
      value match {
        case null                                        => 0.0
        case value: BigDecimal                           => value.toDouble
        case value: Number                               => value.doubleValue()
        case value if value.equals("-")                  => 0.0
        case value if value.toString.contains("#DIV/0!") => 0.0
        case value if value.toString.trim.isEmpty        => 0.0
        case value                                       => value.toString.toDouble
      }
    } catch {
      case e =>
        println(e.getMessage)
        0.0
    }
  }

}
