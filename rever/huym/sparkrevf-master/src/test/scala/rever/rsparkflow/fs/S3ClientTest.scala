package rever.rsparkflow.fs

import org.scalatest.FunSuite
import rever.rsparkflow.spark.domain.S3FsConfig

import java.io.ByteArrayInputStream

/**
  * @author anhlt - aka andy
**/
class S3ClientTest extends FunSuite {

  private val s3Config = S3FsConfig(
    accessKey = "AKIAWKIXPEEAN6S4A45C",
    secretKey = "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",
    region = "ap-southeast-1",
    bucket = "rever-analysis",
    parentPath = "temp"
  )

  private val fsClient = FsClient.client(s3Config)

  test("Write file to S3") {

    val response = fsClient.writeObject(
      "hello.csv",
      new ByteArrayInputStream("you".getBytes),
      Some(3)
    )

    println(response)
  }

//  test("Get size of bucket") {
//
//    val response = fsClient.estimateSizeInBytes("spark_jobs/")
//
//    println(response)
//
//    assertResult(true)(response.getOrElse(0L) > 0)
//  }

//  test("Estimate no of partition for bucket") {
//
//    val fileCount = fsClient.listObjects(Some("spark_jobs/")).size
//    val sizeOpt = fsClient.estimateSizeInBytes("spark_jobs/")
//    val partition = Utils.estimateNoOfPartitions(sizeOpt.getOrElse(0), 64 * 1024 * 1024)
//
//    println(s"File count: ${fileCount}")
//    println(s"Total size: ${sizeOpt.getOrElse(0L) / (1024 * 1024)} MB")
//    println(s"No of partitions: ${partition}")
//
//    assertResult(true)(sizeOpt.getOrElse(0L) > 0)
//
//  }

  test("Check bucket exists 2") {

    val response = fsClient.existBucket("spark_jobs/")

    println(response)

    assertResult(true)(response)
  }

}
