import scala.util.matching.Regex

object StringTest {
  def main(args: Array[String]): Unit = {
    val s = "ruby ruby"
    val Geeks = new Regex("([Rr]uby(, )?)+")
    val y = " is a CS portal. I like KfG, P002-01"

    // Displays all the matches separated
    // by a separator
    println((Geeks.findFirstIn(s)))
  }

}
