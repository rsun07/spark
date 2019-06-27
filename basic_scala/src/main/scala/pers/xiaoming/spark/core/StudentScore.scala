package pers.xiaoming.spark.core

class StudentScore(val name:String,
                   val total:Int,
                   val math:Int,
                   val english:Int) extends Serializable {

  override def toString = s"StudentScore($name, $total, $math, $english)"
}
