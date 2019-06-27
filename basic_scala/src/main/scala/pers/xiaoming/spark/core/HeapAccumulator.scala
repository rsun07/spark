package pers.xiaoming.spark.core

import Ordering.Implicits._
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable
import scala.reflect.ClassTag

class HeapAccumulator[T : Ordering : ClassTag](private val n:Int, private val heap:mutable.PriorityQueue[T])
  extends AccumulatorV2[T, List[T]] {

  def this(n:Int) = this(n, mutable.PriorityQueue.empty[T](Ordering[T].reverse))

  override def isZero: Boolean = heap.isEmpty

  override def copy(): AccumulatorV2[T, List[T]] = new HeapAccumulator[T](n, heap)

  override def reset(): Unit = heap.clear()

  override def add(v: T): Unit = {
    if (heap.size < n) {
      heap.+=(v)
    } else {
      if (heap.head < v) {
        heap.dequeue()
        heap.+=(v)
      }
    }
  }

  override def merge(other: AccumulatorV2[T, List[T]]): Unit = {
    other.value.foreach(heap.+=)
  }

  override def value: List[T] = heap.toList
}
