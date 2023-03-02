package learning_api.util

import java.io.{File, PrintStream}

import com.twitter.util.Try
import com.typesafe.config._

import scala.collection.JavaConverters._

/**
  * Created by sonpn
  */
object ZConfig {

  private final val APP_MODE_VAR_NAME = "mode"

  private val modeFromProp = System.getProperty(APP_MODE_VAR_NAME)
  private val modeFromEnv = System.getenv(APP_MODE_VAR_NAME)

  val appMode: String = Option(modeFromProp).orElse(Option(modeFromEnv)).getOrElse("development")
  val appName: String = Option(System.getProperty("name")).orElse(Option(System.getenv("name"))).getOrElse("<unknown>")

  private lazy val configFile = {
    println(
      s"""
         |Loading app mode:
         |  - From properties: $modeFromProp
         |  - From ENV: $modeFromEnv
         |Use mode: $appMode
       """.stripMargin)
    s"conf/$appMode.conf"
  }
  private val file = new File(configFile)
  println(s"Config file path: ${file.getAbsolutePath}")

  val config: Config = ConfigFactory.load().withFallback(ConfigFactory.parseFile(file))

  def optConf(s: String): Option[Config] = if (config.hasPath(s)) Some(config.getConfig(s)) else None

  def getConf(s: String): Config = config.getConfig(s)

  def getInt(s: String): Int = config.getInt(s)

  def getInt(s: String, default: Int): Int = if (hasPath(s)) getInt(s) else default

  def getIntList(s: String): Seq[Int] = config.getIntList(s).asScala.toIndexedSeq.map(x => x.toInt)

  def getIntList(s: String, default: Seq[Int]): Seq[Int] = if (hasPath(s)) getIntList(s) else default

  def getDouble(s: String): Double = config.getDouble(s)

  def getDouble(s: String, default: Double): Double = if (hasPath(s)) getDouble(s) else default

  def getDoubleList(s: String): List[Double] = config.getDoubleList(s).asScala.toList.map(x => x.toDouble)

  def getDoubleList(s: String, default: List[Double]): List[Double] = if (hasPath(s)) getDoubleList(s) else default

  def getLong(s: String): Long = config.getLong(s)

  def getLong(s: String, default: Long): Long = if (hasPath(s)) getLong(s) else default

  def getLongList(s: String): Seq[Long] = config.getLongList(s).asScala.toList.map(x => x.toLong)

  def getLongList(s: String, default: List[Long]): Seq[Long] = if (hasPath(s)) getLongList(s) else default

  def getBoolean(s: String): Boolean = config.getBoolean(s)

  def getBoolean(s: String, default: Boolean): Boolean = if (hasPath(s)) getBoolean(s) else default

  def getBooleanList(s: String): List[Boolean] = config.getBooleanList(s).asScala.toList.map(x => x.booleanValue())

  def getBooleanList(s: String, default: List[Boolean]): List[Boolean] = if (hasPath(s)) getBooleanList(s) else default

  def optString(s: String): Option[String] = if (hasPath(s)) Some(config.getString(s)) else None

  def getString(s: String): String = config.getString(s)

  def getString(s: String, default: String): String = if (hasPath(s)) getString(s) else default

  def getStringList(s: String): List[String] = config.getStringList(s).asScala.toList

  def getStringList(s: String, default: List[String]): List[String] = if (hasPath(s)) getStringList(s) else default

  def getStringSeq(s: String): Seq[String] = config.getStringList(s).asScala

  def getStringSeq(s: String, default: Seq[String]): Seq[String] = if (hasPath(s)) getStringSeq(s) else default

  def getIsNull(s: String): Boolean = config.getIsNull(s)

  def hasPath(s: String): Boolean = config.hasPath(s)

  def getMap(s: String): Map[String, AnyRef] = config.getObject(s).unwrapped().asScala.toMap

  def getSeqMap(s: String): Seq[Map[String, AnyRef]] = config.getObjectList(s).asScala.map(_.unwrapped().asScala.toMap)

  def getSeqMap(s: String, default: Seq[Map[String, AnyRef]]): Seq[Map[String, AnyRef]] = {
    if (hasPath(s)) getSeqMap(s) else default
  }

  def getMapString(s: String, default: Map[String, String]): Map[String, String] = {
    if (hasPath(s)) getMap(s).map(f => f._1 -> f._2.toString) else default
  }

  def getMapStringList(s: String, defaultValue: Map[String, Seq[String]] = Map()): Map[String, Seq[String]] = {
    if (hasPath(s)) Try(getMap(s).map(f => f._1 -> f._2.asInstanceOf[java.util.List[String]].asScala)).getOrElse(defaultValue)
    else defaultValue
  }

  def print(ps: PrintStream = System.out) = config.entrySet().asScala.foreach(x => {
    ps.println(x.getKey + "=" + String.valueOf(x.getValue))
  })

  implicit class ImplicitConfig(conf: Config) {

    def optConf(s: String): Option[Config] = if (conf.hasPath(s)) Some(conf.getConfig(s)) else None

    def getConf(s: String, default: Config): Config = if (conf.hasPath(s)) conf.getConfig(s) else default

    def getConf(s: String): Config = conf.getConfig(s)

    def getInt(s: String): Int = conf.getInt(s)

    def getInt(s: String, default: Int): Int = if (hasPath(s)) getInt(s) else default

    def getIntList(s: String): Seq[Int] = conf.getIntList(s).asScala.toIndexedSeq.map(x => x.toInt)

    def getIntList(s: String, default: Seq[Int]): Seq[Int] = if (hasPath(s)) getIntList(s) else default

    def getDouble(s: String): Double = conf.getDouble(s)

    def getDouble(s: String, default: Double): Double = if (hasPath(s)) getDouble(s) else default

    def getDoubleList(s: String): List[Double] = conf.getDoubleList(s).asScala.toList.map(x => x.toDouble)

    def getDoubleList(s: String, default: List[Double]): List[Double] = if (hasPath(s)) getDoubleList(s) else default

    def getLong(s: String): Long = conf.getLong(s)

    def getLong(s: String, default: Long): Long = if (hasPath(s)) getLong(s) else default

    def getLongList(s: String): Seq[Long] = conf.getLongList(s).asScala.toList.map(x => x.toLong)

    def getLongList(s: String, default: List[Long]): Seq[Long] = if (hasPath(s)) getLongList(s) else default

    def getBoolean(s: String): Boolean = conf.getBoolean(s)

    def getBoolean(s: String, default: Boolean): Boolean = if (hasPath(s)) getBoolean(s) else default

    def getBooleanList(s: String): List[Boolean] = conf.getBooleanList(s).asScala.toList.map(x => x.booleanValue())

    def getBooleanList(s: String, default: List[Boolean]): List[Boolean] = if (hasPath(s)) getBooleanList(s) else default

    def optString(s: String): Option[String] = if (hasPath(s)) Some(conf.getString(s)) else None

    def getString(s: String): String = conf.getString(s)

    def getString(s: String, default: String): String = if (hasPath(s)) getString(s) else default

    def getStringList(s: String): List[String] = conf.getStringList(s).asScala.toList

    def getStringList(s: String, default: List[String]): List[String] = if (hasPath(s)) getStringList(s) else default

    def getStringSeq(s: String): Seq[String] = conf.getStringList(s).asScala

    def getStringSeq(s: String, default: Seq[String]): Seq[String] = if (hasPath(s)) getStringSeq(s) else default

    def getIsNull(s: String): Boolean = conf.getIsNull(s)

    def hasPath(s: String): Boolean = conf.hasPath(s)

    def getMap(s: String): Map[String, AnyRef] = conf.getObject(s).unwrapped().asScala.toMap

    def getMapString(s: String, default: Map[String, String]): Map[String, String] = {
      if (hasPath(s)) getMap(s).map(f => f._1 -> f._2.toString) else default
    }

    def getMapStringList(s: String, defaultValue: Map[String, Seq[String]] = Map()): Map[String, Seq[String]] = {
      if (hasPath(s)) Try(getMap(s).map(f => f._1 -> f._2.asInstanceOf[java.util.List[String]].asScala)).getOrElse(defaultValue)
      else defaultValue
    }

    def getSeqMap(s: String): Seq[Map[String, AnyRef]] = conf.getObjectList(s).asScala.map(_.unwrapped().asScala.toMap)

    def getSeqMap(s: String, default: Seq[Map[String, AnyRef]]): Seq[Map[String, AnyRef]] = {
      if (hasPath(s)) getSeqMap(s) else default
    }

    def print(ps: PrintStream = System.out) = conf.entrySet().asScala.foreach(x => {
      ps.println(x.getKey + "=" + String.valueOf(x.getValue))
    })
  }

}
