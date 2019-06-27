package pers.xiaoming.spark.core

import org.apache.spark.SparkContext
import org.apache.spark.util.AccumulatorV2

import scala.reflect.ClassTag

class TopNSelector(private val sc:SparkContext) extends AutoCloseable{

  def getTopNSortImpl[T : Ordering : ClassTag](input:List[T], n:Int): List[T] = {
    val inputRDD = sc.parallelize(input)
      inputRDD.sortBy[T](i => i, false, 1).take(n).toList
  }

  def getTopNHeapImpl[T : Ordering : ClassTag](input:List[T], n:Int): List[T] = {
    val inputRDD = sc.parallelize(input)

    val heap:AccumulatorV2[T, List[T]] = new HeapAccumulator[T](n)
    sc.register(heap, "Heap")
    inputRDD.foreach(heap.add)
    heap.value
  }

  override def close(): Unit = sc.stop()
}
