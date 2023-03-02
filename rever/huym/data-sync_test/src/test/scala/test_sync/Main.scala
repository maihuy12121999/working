package test_sync

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import rever.etl.data_sync.util.Utils.ImplicitAny
object Main {
  final val mySqlConfig = new HikariConfig("config/mysql_config.properties")
  final val dataSource = new HikariDataSource(mySqlConfig)
  final val con = dataSource.getConnection
  def main(args: Array[String]): Unit = {
    val mySqlStudentDAO = MySqlStudentDAO(dataSource)
    val delete  = mySqlStudentDAO.deleteAll()
    val insert = mySqlStudentDAO.insert(StudentRecord(1.asOpt,"huy".asOpt,23.asOpt,"12a8".asOpt,"mac dinh chi".asOpt))
    val resultSet = con.prepareStatement("select * from student").executeQuery()
    println(mySqlStudentDAO.select(StudentRecord(1.asOpt)))
    println(mySqlStudentDAO.executeUpdate("update student set name = ?, age = ?",Seq("nhi",18),1)(con))
    println(mySqlStudentDAO.insertOrUpdate(StudentRecord(2.asOpt,"tuyen".asOpt,23.asOpt,"12a8".asOpt,"mac dinh chi".asOpt)))
    println(mySqlStudentDAO.insertOrUpdate(StudentRecord(3.asOpt,"tuyen".asOpt,23.asOpt,"12a8".asOpt,"mac dinh chi".asOpt)))
    println(mySqlStudentDAO.countAll())
    mySqlStudentDAO.executeQuery[]()
  }
}
