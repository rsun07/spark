package pers.xiaoming.spark.transformation_and_action

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{AfterClass, BeforeClass}

class DemoBase {

}

object DemoBase {
  var sc:SparkContext = _

  @BeforeClass
  def setup: Unit = {
    val conf = new SparkConf().setAppName("BasicDemo").setMaster("local")
    sc = new SparkContext(conf)
  }

  @AfterClass
  def close: Unit = {
    sc.stop()
  }
}
