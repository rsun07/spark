package pers.xiaoming.spark.transformation_and_action

import org.junit.Test
import pers.xiaoming.spark.SparkCoreDemoTestBase

class ActionDemo extends SparkCoreDemoTestBase {
  val words = Array("A", "B", "C", "D", "E")
  val wordsRDD = SparkCoreDemoTestBase.sc.parallelize(words)

  val nums = Array(1, 2, 3, 4, 5)
  val numsRDD = SparkCoreDemoTestBase.sc.parallelize(nums)

  @Test
  def reduceDemo: Unit = {
    val result = wordsRDD.reduce((s1, s2) => String.format("(%s + %s)", s1, s2))
    println(result)

    val sum = numsRDD.reduce(_ + _)
    println(result)
  }

  @Test
  def collectDemo: Unit = {
    val collectedWords = wordsRDD.collect()
    println(collectedWords)

    val collectedNums = wordsRDD.collect()
    println(collectedNums)
  }

  @Test
  def countDemo: Unit = {
    val numOfWords = wordsRDD.count()
    println(numOfWords)

    val numOfNums = wordsRDD.count()
    println(numOfNums)
  }

  @Test
  def takeDemo: Unit = {
    val first3Words = wordsRDD.take(3)
    println(first3Words)

    val first3Nums = wordsRDD.take(3)
    println(first3Nums)
  }

  @Test
  def saveAsTextFileDemo: Unit = {
    wordsRDD.saveAsTextFile("hdfs://localhost:9000/words.txt")

    numsRDD.saveAsTextFile("hdfs://localhost:9000/nums.txt")
  }

  @Test
  def countByKeyDemo(): Unit = {
    val employeeWithDepartment = Array(
      Tuple2("Dev", "A"),
      Tuple2("HR", "B"),
      Tuple2("Dev", "C"),
      Tuple2("Finance", "D"),
      Tuple2("HR", "E"),
      Tuple2("Dev", "F"),
      Tuple2("HR", "G"),
      Tuple2("HR", "H"),
      Tuple2("Finance", "I"))

    val employeeRDD = SparkCoreDemoTestBase.sc.parallelize(employeeWithDepartment, 1)

    val numOfEmployeeEachDep = employeeRDD.countByKey()
    println(numOfEmployeeEachDep)
  }
}
