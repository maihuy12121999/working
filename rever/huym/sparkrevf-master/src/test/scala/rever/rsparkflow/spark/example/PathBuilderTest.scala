package rever.rsparkflow.spark.example

import org.scalatest.FunSuite
import rever.rsparkflow.spark.utils.PathBuilder

/**
  * @author anhlt - aka andy
**/
class PathBuilderTest extends FunSuite {
  test("Get path segments") {
    val pathBuilder = PathBuilder.parquet("rever-analysis/temp")

    val list = pathBuilder.getPathSegments("s3a://rever-analysis/temp/An/oppo_distribution/2022/04/21_04_2022.parquet")

    println(list.toArray.mkString(", "))
  }

  test("Parquet An path") {
    val pathBuilder = PathBuilder.parquet("s3a://rever-analysis/temp")
    assertResult("s3a://rever-analysis/temp/An/oppo_distribution/2022/04/21_04_2022.parquet")(
      pathBuilder.buildAnPath("oppo_distribution", 1650521311000L)
    )
  }

  test("Parquet A0 path") {
    val pathBuilder = PathBuilder.parquet("s3a://rever-analysis/temp")
    assertResult("s3a://rever-analysis/temp/A0/oppo_distribution/2022/04/21_04_2022.parquet")(
      pathBuilder.buildA0Path("oppo_distribution", 1650521311000L)
    )
  }

  test("Parquet A1 path") {
    val pathBuilder = PathBuilder.parquet("s3a://rever-analysis/temp")
    assertResult("s3a://rever-analysis/temp/A1/oppo_distribution/2022/04/21_04_2022.parquet")(
      pathBuilder.buildA1Path("oppo_distribution", 1650521311000L)
    )
  }

  test("Parquet A7 path") {
    val pathBuilder = PathBuilder.parquet("s3a://rever-analysis/temp")
    assertResult("s3a://rever-analysis/temp/A7/oppo_distribution/2022/04/21_04_2022.parquet")(
      pathBuilder.buildA7Path("oppo_distribution", 1650521311000L)
    )
  }

  test("Parquet A30 path") {
    val pathBuilder = PathBuilder.parquet("s3a://rever-analysis/temp")
    assertResult("s3a://rever-analysis/temp/A30/oppo_distribution/2022/04/21_04_2022.parquet")(
      pathBuilder.buildA30Path("oppo_distribution", 1650521311000L)
    )
  }

  test("CSV An path") {
    val pathBuilder = PathBuilder.csv("s3a://rever-analysis/temp")
    assertResult("s3a://rever-analysis/temp/An/oppo_distribution/2022/04/21_04_2022.csv")(
      pathBuilder.buildAnPath("oppo_distribution", 1650521311000L)
    )
  }

  test("CSV A0 path") {
    val pathBuilder = PathBuilder.csv("s3a://rever-analysis/temp")
    assertResult("s3a://rever-analysis/temp/A0/oppo_distribution/2022/04/21_04_2022.csv")(
      pathBuilder.buildA0Path("oppo_distribution", 1650521311000L)
    )
  }

  test("CSV A1 path") {
    val pathBuilder = PathBuilder.csv("s3a://rever-analysis/temp")
    assertResult("s3a://rever-analysis/temp/A1/oppo_distribution/2022/04/21_04_2022.csv")(
      pathBuilder.buildA1Path("oppo_distribution", 1650521311000L)
    )
  }

  test("CSV A7 path") {
    val pathBuilder = PathBuilder.csv("s3a://rever-analysis/temp")
    assertResult("s3a://rever-analysis/temp/A7/oppo_distribution/2022/04/21_04_2022.csv")(
      pathBuilder.buildA7Path("oppo_distribution", 1650521311000L)
    )
  }

  test("CSV A30 path") {
    val pathBuilder = PathBuilder.csv("s3a://rever-analysis/temp")
    assertResult("s3a://rever-analysis/temp/A30/oppo_distribution/2022/04/21_04_2022.csv")(
      pathBuilder.buildA30Path("oppo_distribution", 1650521311000L)
    )
  }

  test("JSON An path") {
    val pathBuilder = PathBuilder.json("s3a://rever-analysis/temp")
    assertResult("s3a://rever-analysis/temp/An/oppo_distribution/2022/04/21_04_2022.json")(
      pathBuilder.buildAnPath("oppo_distribution", 1650521311000L)
    )
  }

  test("JSON A0 path") {
    val pathBuilder = PathBuilder.json("s3a://rever-analysis/temp")
    assertResult("s3a://rever-analysis/temp/A0/oppo_distribution/2022/04/21_04_2022.json")(
      pathBuilder.buildA0Path("oppo_distribution", 1650521311000L)
    )
  }

  test("JSON A1 path") {
    val pathBuilder = PathBuilder.json("s3a://rever-analysis/temp")
    assertResult("s3a://rever-analysis/temp/A1/oppo_distribution/2022/04/21_04_2022.json")(
      pathBuilder.buildA1Path("oppo_distribution", 1650521311000L)
    )
  }

  test("JSON A7 path") {
    val pathBuilder = PathBuilder.json("s3a://rever-analysis/temp")
    assertResult("s3a://rever-analysis/temp/A7/oppo_distribution/2022/04/21_04_2022.json")(
      pathBuilder.buildA7Path("oppo_distribution", 1650521311000L)
    )
  }

  test("JSON A30 path") {
    val pathBuilder = PathBuilder.json("s3a://rever-analysis/temp")
    assertResult("s3a://rever-analysis/temp/A30/oppo_distribution/2022/04/21_04_2022.json")(
      pathBuilder.buildA30Path("oppo_distribution", 1650521311000L)
    )
  }

  test("Parquet An path with empty parent") {
    val pathBuilder = PathBuilder.parquet("")
    assertResult("An/oppo_distribution/2022/04/21_04_2022.parquet")(
      pathBuilder.buildAnPath("oppo_distribution", 1650521311000L)
    )
  }
}
