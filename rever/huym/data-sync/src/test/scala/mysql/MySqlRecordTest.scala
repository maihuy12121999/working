package mysql

import org.apache.spark.sql.SparkSession
import rever.etl.data_sync.core.clickhouse.ClickhouseSink.getDataSource
import rever.etl.data_sync.util.Utils.ImplicitAny
import rever.etl.rsparkflow.api.configuration.{Config, DefaultConfig}
import vn.rever.jdbc.JdbcRecord
import vn.rever.jdbc.mysql.{MySqlDAO, MysqlEngine}

import java.util.regex.Pattern
import javax.sql.DataSource
import scala.jdk.CollectionConverters.mapAsJavaMapConverter

case class MySqlRecordTest(
    var id: Option[String],
    var name: Option[String],
    var updateTime: Option[Long]
) extends JdbcRecord {

  override def getPrimaryKeys(): Seq[String] = Seq(mySqlTestFields.ID)

  override def getFields(): Seq[String] =
    Seq(
      mySqlTestFields.ID,
      mySqlTestFields.NAME,
      mySqlTestFields.UPDATE_TIME
    )

  override def setValues(field: String, value: Any): Unit = {
    field match {
      case mySqlTestFields.ID          => id = value.asOptString
      case mySqlTestFields.NAME        => name = value.asOptString
      case mySqlTestFields.UPDATE_TIME => updateTime = value.asOptLong
    }
  }

  override def getValue(field: String): Option[Any] = {
    field match {
      case mySqlTestFields.ID          => id
      case mySqlTestFields.NAME        => name
      case mySqlTestFields.UPDATE_TIME => updateTime
    }
  }
}

case class MySqlDAOTest(ds: DataSource) extends MySqlDAO[MySqlRecordTest] {

  override def createRecord(): MySqlRecordTest = {
    MySqlRecordTest(None, None, None)
  }

  override def table: String = "updated_employee"
}
object MySqlRecordTestRunner {
  def main(args: Array[String]): Unit = {
    val config = new DefaultConfig(
      Map[String, AnyRef](
        "HOST" -> "localhost",
        "PORT" -> "3306",
        "USERNAME" -> "root",
        "PASSWORD" -> "password",
        "DRIVER" -> "com.mysql.cj.jdbc.Driver",
        "DBNAME" -> "increase_data_sync_test",
        "DB_TABLE" -> "updated_employee"
      ).asJava
    )

    val ds = getDataSource(config)
    val mysqlDao = MySqlDAOTest(ds)

//    val test = mysqlDao
//      .selectList(0, 4)
//      .map(record => MySqlRecordTest(record.id, record.name, record.updateTime).getValues())
//
//    val testDelete = mysqlDao
//      .selectList(0, 4)
//      .map(record => {
//        val id = record.id.getOrElse("")
//        val updateTime = record.updateTime.getOrElse(0)
//        if (id == "1" && (updateTime == 1 || updateTime == 0))
//          mysqlDao.delete(MySqlRecordTest(record.id, record.name, record.updateTime))
//      })

//    val testUpdate = mysqlDao
//      .selectList(0, 4)
//      .map(record => {
//        val name = record.name.getOrElse("")
//        if (name.forall(Character.isDigit))
//          mysqlDao.update(
//            MySqlRecordTest(record.id, Some("unknown"), record.updateTime)
//          )
//      })

    val testSetValue = mysqlDao
      .selectList(0, 4)
      .map(record => {
          MySqlRecordTest(record.id, record.name, record.updateTime).setValues("name","unknown")
        MySqlRecordTest(record.id, record.name, record.updateTime)
      })
//    mysqlDao.
//
//    println(testSetValue)

  }

  private def getDataSource(config: Config): DataSource = {
    val host = config.get("HOST")
    val port = config.get("PORT")
    val driver = config.get("DRIVER")
    val user = config.get("USERNAME")
    val password = config.get("PASSWORD")
    val dbName = config.get("DBNAME")
    MysqlEngine
      .getDataSource(
        driver,
        host,
        port.toInt,
        dbName,
        user,
        password
      )
  }

}
