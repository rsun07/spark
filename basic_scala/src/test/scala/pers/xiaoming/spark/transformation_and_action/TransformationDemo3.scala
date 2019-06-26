package pers.xiaoming.spark.transformation_and_action

import org.junit.Test

class TransformationDemo3 extends DemoBase {
  private val listOfIdToName = Array((1, "A"), (2, "B"), (3, "C"))
  private val listOfIdToEmail = Array(
    (1, "A@test.c"), (2, "B@test.c"), (3, "C@test.c"))
  private val listOfIdToEmails = Array(
    (1, "A@test.c"), (2, "B@test.c"), (3, "C@test.c"),
    (1, "A2@test.c"), (2, "B2@test.c"), (3, "C2@test.c"))

  private val idToNameRDD = DemoBase.sc.parallelize(listOfIdToName)
  private val idToEmailRDD = DemoBase.sc.parallelize(listOfIdToEmail)
  private val idToEmailsRDD = DemoBase.sc.parallelize(listOfIdToEmails)

  @Test
  def joinDemo: Unit = {
    idToNameRDD.join(idToEmailRDD).foreach(println)

    idToNameRDD.join(idToEmailsRDD).foreach(println)
  }

  @Test
  def cogroupDemo: Unit = {
    idToNameRDD.cogroup(idToEmailRDD).foreach(println)

    idToNameRDD.cogroup(idToEmailsRDD).foreach(println)
  }
}
