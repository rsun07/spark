package pers.xiaoming.spark.transformation_and_action

import org.junit.Test

class TransformationDemo2 extends DemoBase {

  private val employeeWithDepartment = Array(
    Tuple2("Dev", "A"),
    Tuple2("HR", "B"),
    Tuple2("Dev", "C"),
    Tuple2("Finance", "D"),
    Tuple2("HR", "E"),
    Tuple2("Dev", "F"),
    Tuple2("HR", "G"),
    Tuple2("HR", "H"),
    Tuple2("Finance", "I"))

  private val employees = DemoBase.sc.parallelize(employeeWithDepartment, 1)

  @Test
  def groupByKeyDemo: Unit = {
    employees.groupByKey()
      .foreach(tuple => {
        print(tuple._1 + " : " + tuple._2.toString())
        println
      })
  }

  @Test
  def reduceByKeyDemo: Unit = {
    employees.reduceByKey((s1, s2) => String.format("(%s + %s)", s1, s2))
      .foreach(tuple => {
        print(tuple._1 + " : " + tuple._2)
        println
      })
  }

  @Test
  def sortByKeyDemo: Unit = {
    employees.sortByKey().foreach(println)
  }
}
