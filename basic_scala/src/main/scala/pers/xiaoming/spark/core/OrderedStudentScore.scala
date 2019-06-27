package pers.xiaoming.spark.core

class OrderedStudentScore(name:String,total:Int,math:Int,english:Int) extends StudentScore(name:String,total:Int,math:Int,english:Int) with Ordered[OrderedStudentScore] with Serializable {

  override def compare(that: OrderedStudentScore): Int = {
    if (this.total - that.total != 0) {
      this.total - that.total
    } else {
      this.math - that.math
    }
  }

  override def toString = s"OrderedStudentScore($name, $total, $math, $english)"
}
