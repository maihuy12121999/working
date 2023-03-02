import java.net.URL

/** @author anhlt (andy)
  * @since 27/04/2022
  */
object DomainParserTest {
  def main(args: Array[String]): Unit = {
    val urls = Seq(
      "https://rever.vn/chu-dau-tu/novaland",
      "https://rever.vn",
      "https://blog.rever.vn/cap-nhat-gia-ban-cac-chung-cu-quan-4-thoi-diem-thang-5-2021",
      "https://blog.rever.vn/"
    )

    val hosts = urls.map(url => {
      val u = new URL(url)
      u.getHost
    })

    println(hosts.mkString(", "))

  }
}
