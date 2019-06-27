package pers.xiaoming.spark.core

import org.apache.spark.SparkContext
import org.apache.spark.util.AccumulatorV2

class TopNSelector(private val sc:SparkContext) extends AutoCloseable{

  def getTOpNSortImpl[T <: Comparable[T]](input:List[T], n:Int): (T => T) => Array[T] = {
    sc.parallelize(input).sortBy[T](_, false, 1).take(n)
  }

  def getTopNHeapImpl[T <: Comparable[T]](input:List[T], n:Int): Array[T] = {
    val inputRDD = sc.parallelize(input)

    val heap:AccumulatorV2[T, Array[T]] = new HeapAccumulator[T](n)
    sc.register(heap, "Heap")
    inputRDD.foreach(heap.add)
    heap.value
  }

  override def close(): Unit = sc.stop()
}
