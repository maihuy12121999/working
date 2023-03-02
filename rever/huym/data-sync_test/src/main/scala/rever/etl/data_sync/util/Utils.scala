package rever.etl.data_sync.util

/** @author anhlt (andy)
  * @since 09/07/2022
  */
object Utils {
  implicit class ImplicitAny(val value: Any) extends AnyVal {
    def asOpt[T]: Option[T] = value.asOptAny.map(_.asInstanceOf[T])

    def asOptInt: Option[Int] = value.asOptAny.map(_.asInstanceOf[Int])

    def asOptLong: Option[Long] = value.asOptAny.map(_.asInstanceOf[Long])

    def asOptString: Option[String] = value.asOptAny.map(_.asInstanceOf[String])

    def asOptDouble: Option[Double] = value.asOptAny.map(_.asInstanceOf[Double])

    def asOptFloat: Option[Float] = value.asOptAny.map(_.asInstanceOf[Float])

    def asOptBoolean: Option[Boolean] = value.asOptAny.map(_.asInstanceOf[Boolean])

    def asOptShort: Option[Short] = value.asOptAny.map(_.asInstanceOf[Short])

    def asOptAny: Option[Any] = value match {
      case s: Option[_] => s
      case _            => Option(value)
    }

    def orEmpty: String = value match {
      case Some(x) => x.toString
      case _       => value.toString
    }
  }

}
