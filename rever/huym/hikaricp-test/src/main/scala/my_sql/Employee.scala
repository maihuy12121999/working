package scala.my_sql

case class Employee(var id:Int = 0, var name:String="", var age:Int=0, var title:String="", var department:String="",var manager:String=""){
  def show(): Unit={
    println(s"Employee(id=$id,name=$name,age=$age,title=$title,department=$department,manager=$manager)")
  }
}
