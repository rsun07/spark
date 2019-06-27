package pers.xiaoming.spark.core

import org.junit.Test
import pers.xiaoming.spark.SparkCoreDemoTestBase

class VariableDemo extends SparkCoreDemoTestBase {
  val nums = Array(1, 2, 3)
  val numsRDD = SparkCoreDemoTestBase.sc.parallelize(nums, 1)

  @Test
  def broadcastVariableDemo: Unit = {
    val base = 2
    val baseBroadcase = SparkCoreDemoTestBase.sc.broadcast(2)

    // In this case, the integer 'base' will be copied to every node and every executor's tasks
    // If the object need to be shared is large, then it's a wast of resources
    val result = numsRDD.filter(x => x % base == 0).collect()

    // In this case, the 'baseBroadcast' will be copied only once in each node
    // And shared by all executor's tasks
    val result2 = numsRDD.filter(x => x % baseBroadcase.value == 0).collect()
  }

  @Test
  def accumulatorVariableDemo: Unit = {
    val sum = new MyAccumulator()
    SparkCoreDemoTestBase.sc.register(sum, "SUM")

    numsRDD.foreach(num => sum.add(num.toLong))
    println(sum)
  }
}
