package pers.xiaoming.spark.core

class StudentScore(val name:String,
                   private val total:Int,
                   private val math:Int,
                   private val english:Int) extends Ordered[StudentScore] with Serializable {

  override def compare(that: StudentScore): Int = {
    if (this.total - that.total != 0) {
      this.total - that.total
    } else {
      this.math - that.math
    }
  }

  override def toString = s"StudentScore($name, $total, $math, $english)"
}
