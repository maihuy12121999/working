package scala.my_sql

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}
import scala.collection.mutable.ListBuffer
import java.util.UUID
import scala.util.matching.Regex
object Main {
  final val mySqlConfig = new HikariConfig(s"config/mysql_config.properties")
  final val dataSource: HikariDataSource =  new HikariDataSource(mySqlConfig)
  def main(args: Array[String]): Unit = {
    val employee = Employee(id = 1,name = "Phan Tan Trung",age = 33,title = "streamer",department = "sbtc",manager = "Batus")
    if(insertEmployee(employee)>0) {
      val employees = fetchData()
      employees.foreach(_.show())
    }
    try{
      val employee = Employee(id = 1,name = "Mai Huy",age = 23,title = "data engineer",department = "data",manager = "LTA")
      updateEmployee(employee)
    }
    catch{
      case e:SQLException=>
        e.printStackTrace()
    }
  }
  def fetchData():ListBuffer[Employee] ={
    val sqlQuery = initQuery()
    val employees = ListBuffer.empty[Employee]
    var con:Connection =null
    var pst:PreparedStatement = null
    var resultSet:ResultSet = null
    try {
      con = dataSource.getConnection
      pst = con.prepareStatement(sqlQuery)
      resultSet = pst.executeQuery()
      while (resultSet.next()) {
        val id = resultSet.getInt("id")
        val name = resultSet.getString("name")
        val age = resultSet.getInt("age")
        val title = resultSet.getString("title")
        val department = resultSet.getString("department")
        val manager = resultSet.getString("manager")
        val employee = Employee(id,name,age,title,department,manager)
        employees.append(employee)
      }
    }
    catch{
      case e:SQLException=>
        e.printStackTrace()
    }
    finally{
      if(con !=null) con.close()
      if(pst!=null) pst.close()
      if(resultSet!=null) resultSet.close()
    }
    employees
  }
  def insertEmployee(employee: Employee): Int ={
    val sql = insertEmployeeSQL()
    var con:Connection =null
    var pst:PreparedStatement = null
    try {
      con = dataSource.getConnection
      pst = con.prepareStatement(sql)
      pst.setInt(1, employee.id)
      pst.setString(2, employee.name)
      pst.setInt(3, employee.age)
      pst.setString(4, employee.title)
      pst.setString(5, employee.department)
      pst.setString(6, employee.manager)
      pst.executeUpdate()
    }
    catch{
      case e:SQLException=>
        e.printStackTrace()
        0
    }
    finally{
      if(con !=null) con.close()
      if(pst!=null) pst.close()
    }
  }
  def deleteEmployee(employeeId:Int): Int ={
    val sql = deleteEmployeeSQL()
    var con:Connection =null
    var pst:PreparedStatement = null
    try {
      con = dataSource.getConnection
      pst = con.prepareStatement(sql)
      pst.setInt(1, employeeId)
      pst.executeUpdate()
    }
    catch{
      case e:SQLException=>
        e.printStackTrace()
        0
    }
    finally{
      if(con !=null) con.close()
      if(pst!=null) pst.close()
    }
  }
  def updateEmployee(employee:Employee): Int ={
    val sql = updateEmployeeSql()
    var con:Connection =null
    var pst:PreparedStatement = null
    try {
      con = dataSource.getConnection
      pst = con.prepareStatement(sql)
      pst.setString(1, employee.name)
      pst.setInt(2, employee.age)
      pst.setString(3, employee.title)
      pst.setString(4, employee.department)
      pst.setString(5, employee.manager)
      pst.setInt(6, employee.id)
      pst.executeUpdate()
    }
    catch{
      case e:SQLException=>
        e.printStackTrace()
        0
    }
    finally{
      if(con !=null) con.close()
      if(pst!=null) pst.close()
    }
  }
  def initQuery():String ={
    s"""
       |select * from employee
       |""".stripMargin
  }
  def insertEmployeeSQL():String={
    s"""
       |insert into employee values(?, ?, ?, ?,?,?)
       |""".stripMargin
  }
  def deleteEmployeeSQL():String={
    s"""
       |delete from employee where id =?;
       |""".stripMargin
  }
  def updateEmployeeSql():String={
    s"""
       |update employee
       |set name = ?, age=?,title = ?, department = ?, manager = ?
       |where id =?;
       |""".stripMargin
  }
}
