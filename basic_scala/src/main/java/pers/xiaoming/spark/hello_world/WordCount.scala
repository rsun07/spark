package pers.xiaoming.spark.hello_world

import org.apache.spark.{SparkConf, SparkContext}

class WordCount(val filePath:String) {
  private val config = new SparkConf().setAppName("Word Count").setMaster("local")
  private val sc = new SparkContext(config)
  private val fileContext = sc.textFile(filePath)

  def wordCount(): Array[(String, Int)] = {
    fileContext.flatMap( line => line.split(" "))
        .map(word => (word, 1))
        .reduceByKey(_ + _)
        .collect()
  }

  def numOfDistinctWords(): Int = {
    fileContext.flatMap(line => line.split(" "))
      .distinct()
      .map(_ => 1)
      .reduce(_ + _)
  }

  def numOfTotalWords(): Int = {
    fileContext.flatMap(line => line.split(" "))
      .map(_ => 1)
      .reduce(_ + _)
  }
}
