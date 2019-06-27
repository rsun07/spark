package pers.xiaoming.spark.core

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class HeapAccumulator[T <: Comparable[T]](private val n:Int, private val heap:mutable.PriorityQueue[T])
  extends AccumulatorV2[T, Array[T]] {

  def this(n:Int) = this(n, new mutable.PriorityQueue[T]())

  override def isZero: Boolean = heap.isEmpty

  override def copy(): AccumulatorV2[T, Array[T]] = new HeapAccumulator[T](n, heap)

  override def reset(): Unit = heap.clear()

  override def add(v: T): Unit = {
    if (heap.size < n) {
      heap.+=(v)
    } else {
      if (heap.head.compareTo(v) > 0) {
        heap.dequeue()
        heap.+=(v)
      }
    }
  }

  override def merge(other: AccumulatorV2[T, Array[T]]): Unit = {
    other.value.foreach(heap.+=)
  }

  override def value: Array[T] = heap.toArray
}
