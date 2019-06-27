package pers.xiaoming.spark.core

import java.io.Serializable
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.util.AccumulatorV2

class MyAccumulator(val initValue:Long = 0) extends AccumulatorV2[Long, Long] with Serializable {

  private val myValue = new AtomicLong(initValue)

  override def isZero: Boolean = myValue.get() == 0

  override def copy(): AccumulatorV2[Long, Long] = new MyAccumulator(myValue.get())

  override def reset(): Unit = myValue.set(0)

  override def add(v: Long): Unit = myValue.addAndGet(v)

  override def merge(other: AccumulatorV2[Long, Long]): Unit = myValue.addAndGet(other.value)

  override def value: Long = myValue.get()
}

