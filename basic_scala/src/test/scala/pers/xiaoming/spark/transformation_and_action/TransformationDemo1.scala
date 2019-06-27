package pers.xiaoming.spark.transformation_and_action

import org.junit.Test
import pers.xiaoming.spark.SparkCoreDemoTestBase

import scala.util.Random

class TransformationDemo1 extends SparkCoreDemoTestBase {
  private val words = List("w1", "w2", "w3")
  private val wordsRDD = SparkCoreDemoTestBase.sc.parallelize(words)


  @Test
  def transformationDemo(): Unit = {
    val upperCaseWordsRDD = wordsRDD.map(_.toUpperCase)

    println("Sleep for 5s, spark job won't be executed until action func is called")

    Thread.sleep(5000)

    upperCaseWordsRDD.foreach(word => print(word + ", "))
    println
  }

  @Test
  def mapDemo: Unit = {
    wordsRDD.map("_" + _ + "_").foreach(word => print(word + ", "))
    println

    val random = new Random()
    wordsRDD.map(_ => random.nextInt(100)).foreach(word => print(word + ", "))
    println

    wordsRDD.map(word => (word, 1)).foreach(word => print(word + ", "))
    println
  }

  // flatMap vs map
  // map: the new RDD has the same number of element as the old RDD
  //      each element is 1 to 1 mapping
  // flatMap: the new RDD could have different number of element as the old RDD
  //         1 to n mapping
  @Test
  def flatMapDemo: Unit = {
    val words = List("w1 w2 w3", "w2 w3", "w3 w1")
    val wordsRDD = SparkCoreDemoTestBase.sc.parallelize(words)

    // split by space
    wordsRDD.flatMap(word => word.split(" ")).foreach(word => print(word + ", "))
    println

    // split every char
    wordsRDD.flatMap(word => word.split("")).foreach(word => print(word + ", "))
    println

    val words2 = List("w1", "word", "word3")
    val wordsRDD2 = SparkCoreDemoTestBase.sc.parallelize(words2)

    wordsRDD2.flatMap(word => word.toCharArray.map(_.hashCode()))
      .foreach(word => print(word + ", "))
    println
  }

  @Test
  def filterDemo: Unit = {
    val words = List("w1", "word", "word3", "a", "wrod")
    val wordsRDD = SparkCoreDemoTestBase.sc.parallelize(words)

    wordsRDD.filter(word => word.startsWith("word")).foreach(word => print(word + ", "))
    println
  }
}
