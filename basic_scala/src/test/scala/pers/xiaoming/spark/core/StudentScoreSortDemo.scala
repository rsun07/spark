package pers.xiaoming.spark.core

import org.junit.Test
import pers.xiaoming.spark.SparkCoreDemoTestBase

class StudentScoreSortDemo extends SparkCoreDemoTestBase {
  @Test
  def demo {
    val studentScores = Array(
      new StudentScore("A", 150, 80, 70),
      new StudentScore("B", 200, 100, 100),
      new StudentScore("C", 150, 80, 70),
      new StudentScore("D", 150, 70, 80),
      new StudentScore("E", 150, 75, 75),
      new StudentScore("F", 170, 100, 70),
      new StudentScore("G", 170, 90, 80),
      new StudentScore("H", 170, 80, 90),
      new StudentScore("I", 190, 90, 100))

    val studentScoreRDD = SparkCoreDemoTestBase.sc.parallelize(studentScores)

    studentScoreRDD.map(studentScore => (studentScore, studentScore.name))
      .sortByKey(false).foreach(tuple => println(tuple._1))
  }
}
