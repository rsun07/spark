package pers.xiaoming.spark.hello_world

import org.junit.{AfterClass, Assert, BeforeClass, Test}

class WordCountTest {
  @Test
  def testWordCount: Unit = {
    val result = WordCountTest.wordCount.wordCount
    result.foreach(tuple => println(tuple.toString()))
  }

  @Test
  def testNumOfDistinctWord: Unit = {
    val num = WordCountTest.wordCount.numOfDistinctWords
    Assert.assertSame(3, num)
  }

  @Test
  def testNumOfTotalWord: Unit = {
    val num = WordCountTest.wordCount.numOfTotalWords
    Assert.assertSame(9, num)
  }
}

object WordCountTest {
  private val TEST_FILE_PATH = "./src/test/resources/wordcount.txt"
  private var wordCount:WordCount = _

  @BeforeClass
  def init: Unit = {
    wordCount = new WordCount(TEST_FILE_PATH)
  }

  @AfterClass
  def close: Unit = {
    wordCount.shutdown
  }
}
