package rever.etl.data_sync.domain

import rever.etl.data_sync.util.Utils.ImplicitAny
import vn.rever.jdbc.JdbcRecord

case class GenericRecord(
    primaryKeys: Seq[String],
    fields: Seq[String],
    dataMap: Map[String, Any] = Map.empty[String, Any]
) extends JdbcRecord {

  private val internalDataMap = scala.collection.mutable.Map.empty[String, Any]
  dataMap.foreach { case (k, v) =>
    internalDataMap.put(k, v)
  }

  override def getPrimaryKeys(): Seq[String] = primaryKeys

  override def getFields(): Seq[String] = fields

  override def setValues(field: String, value: Any): Unit = {
    value.asOpt.foreach(value => internalDataMap.put(field, value))
  }

  override def getValue(field: String): Option[Any] = {
    internalDataMap.get(field)
  }
}
