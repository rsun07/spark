package pers.xiaoming.spark.core

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class HeapAccumulator(private val n:Int, private val heap:mutable.PriorityQueue[Int])
  extends AccumulatorV2[Int, List[Int]] {

  def this(n:Int) = this(n, new mutable.PriorityQueue[Int]())

  override def isZero: Boolean = heap.isEmpty

  override def copy(): AccumulatorV2[Int, List[Int]] = new HeapAccumulator(n, heap)

  override def reset(): Unit = heap.clear()

  override def add(v: Int): Unit = {
    if (heap.size < n) {
      heap.+=(v)
    } else {
      if (heap.head < v) {
        heap.dequeue()
        heap.+=(v)
      }
    }
  }

  override def merge(other: AccumulatorV2[Int, List[Int]]): Unit = {
    other.value.foreach(heap.+=)
  }

  override def value: List[Int] = heap.toList
}
