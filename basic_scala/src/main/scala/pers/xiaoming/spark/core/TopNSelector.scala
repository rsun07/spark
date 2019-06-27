package pers.xiaoming.spark.core

import org.apache.spark.SparkContext

class TopNSelector(private val sc:SparkContext) {

  def getTOpNSortImpl[T](input:List[T], n:Int): Array[T] = {
    sc.parallelize(input).sortBy[T](_, false, 1).take(n)
  }

  def getTopNHeapImpl[T](input:List[T], n:Int): Array[T] = {

  }
}
