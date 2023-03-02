package rever.rsparkflow.fs

import org.apache.commons.io.FileUtils
import org.scalatest.FunSuite
import rever.rsparkflow.spark.domain.LocalFsConfig
import rever.rsparkflow.spark.utils.Utils

import java.io.ByteArrayInputStream

/**
  * @author anhlt - aka andy
  * */
class LocalFsClientTest extends FunSuite {
  private val fsConfig = LocalFsConfig(
    parentPath = "./data"
  )

  private val fsClient = FsClient.client(fsConfig)

  test("Write file to S3") {

    val response = fsClient.writeObject(
      "data/hello.csv",
      new ByteArrayInputStream("you".getBytes),
      Some(3)
    )

    println(response)
  }

  test("Get size of bucket") {

    val response = fsClient.estimateSizeInBytes("./src")

    println(response)

    assertResult(true)(response.getOrElse(0L) > 0)
  }

  test("Estimate no of partition for bucket") {

    val fileCount = fsClient.listObjects(Some("dist/")).size
    val sizeOpt = fsClient.estimateSizeInBytes("dist/")
    val partition = Utils.estimateNoOfPartitions(sizeOpt.getOrElse(0), 64 * 1024 * 1024)

    println(s"File count: ${fileCount}")
    println(s"Total size: ${FileUtils.byteCountToDisplaySize(sizeOpt.getOrElse(0L))}")
    println(s"No of partitions: ${partition}")

    assertResult(true)(sizeOpt.getOrElse(0L) > 0)

  }

  test("Check bucket exists 2") {

    val response = fsClient.existBucket("dist")

    println(response)

    assertResult(true)(response)
  }

}
