package pers.xiaoming.spark.core

import org.junit.{Assert, BeforeClass, Ignore, Test}
import pers.xiaoming.spark.SparkCoreDemoTestBase

@Ignore
class TopNSelectorTest extends SparkCoreDemoTestBase {
  private val N = 5
  private val topNSelector = new TopNSelector(SparkCoreDemoTestBase.sc)
  private val input = List(1 to 100: _*)
  private val expectResult = List(100, 99, 98, 97, 96)

  // Runtime: 1.56s
  @Test
  def topNSortImplTest: Unit = {
    Assert.assertEquals(expectResult, topNSelector.getTopNSortImpl[Int](input, N))
  }

  // Runtime: 40ms
  @Test
  def topNSortHeapTest: Unit = {
    Assert.assertEquals(expectResult, topNSelector.getTopNHeapImpl[Int](input, N).sorted(Ordering[Int].reverse))
  }
}
