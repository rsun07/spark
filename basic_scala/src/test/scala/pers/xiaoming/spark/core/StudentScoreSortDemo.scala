package pers.xiaoming.spark.core

import org.junit.Test
import pers.xiaoming.spark.SparkCoreDemoTestBase

class StudentScoreSortDemo extends SparkCoreDemoTestBase {
  @Test
  def demo {
    val studentScores = Array(
      new OrderedStudentScore("A", 150, 80, 70),
      new OrderedStudentScore("B", 200, 100, 100),
      new OrderedStudentScore("C", 150, 80, 70),
      new OrderedStudentScore("D", 150, 70, 80),
      new OrderedStudentScore("E", 150, 75, 75),
      new OrderedStudentScore("F", 170, 100, 70),
      new OrderedStudentScore("G", 170, 90, 80),
      new OrderedStudentScore("H", 170, 80, 90),
      new OrderedStudentScore("I", 190, 90, 100))

    val studentScoreRDD = SparkCoreDemoTestBase.sc.parallelize(studentScores)

    studentScoreRDD.map(studentScore => (studentScore, studentScore.name))
      .sortByKey(false).foreach(tuple => println(tuple._1))
  }
}
