package rever.rsparkflow.spark.utils

import org.apache.spark.sql.SparkSession
import rever.rsparkflow.spark.domain.S3FsConfig

import java.util.regex.Pattern

/**
  * @author anhlt (andy)
  * @since 25/04/2022
**/
object Utils {

  def parseArgToMap(args: Array[String]): Map[String, AnyRef] = {

    val dataMap: Map[Int, String] = args.zipWithIndex.map(e => e._2 -> e._1).toMap

    val sparkConfigMap = args.zipWithIndex
      .filter(e => e._1.startsWith("--conf"))
      .map {
        case (k, idx) =>
          dataMap.get(idx + 1) match {
            case Some(v) if !v.startsWith("--") => Some(v)
            case _                              => None
          }

      }
      .toSeq
      .flatten
      .filter(_.indexOf("=") >= 0)
      .map(s => s.split("=", -1))
      .filter(_.length == 2)
      .map(array => array(0) -> array(1))
      .toMap

    val argMap = args.zipWithIndex
      .filter(e => e._1.startsWith("---"))
      .map {
        case (k, idx) =>
          val v = dataMap.get(idx + 1) match {
            case Some(v) if !v.startsWith("---") => Some(v)
            case _                               => None
          }
          k.substring(3) -> v
      }
      .toMap
      .map(e => e._1 -> e._2.getOrElse(""))

    argMap ++ Map("RV_SPARK_CONFIGS" -> JsonUtils.toJson(sparkConfigMap, false))
  }

  def cartesianProduct[T](columnLists: List[List[T]]): List[List[T]] = {
    columnLists match {
      case Nil            => List(List.empty)
      case x if x.isEmpty => List(List.empty)
      case head :: tail   => for (xh <- head; xt <- cartesianProduct(tail)) yield (xh :: xt)
    }
  }

  def applyS3Config(session: SparkSession, s3Config: S3FsConfig): Unit = {
    session.sparkContext.hadoopConfiguration.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
    session.sparkContext.hadoopConfiguration
      .set("fs.s3a.access.key", s3Config.accessKey)
    session.sparkContext.hadoopConfiguration
      .set("fs.s3a.secret.key", s3Config.secretKey)
    session.sparkContext.hadoopConfiguration
      .set("fs.s3a.endpoint", "s3.amazonaws.com")

  }

  def estimateNoOfPartitions(totalSize: Long, partitionSize: Long): Int = {
    totalSize / partitionSize match {
      case x if x < 1 => 1
      case x          => x.toInt
    }
  }

  def isBelongToPackage(packageName: String, clazz: String): Boolean = {
    clazz.matches(s"^${Pattern.quote(packageName)}\\..+")
  }
}
