package org.tritsch.spark.examples

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpec

class WordCountSpec extends FlatSpec with BeforeAndAfterAll {
  import WordCount._

  override def afterAll(): Unit = {
    spark.close()
  }

  "wordCount(lines)" should "return a/the histogram of word counts" in {
    import spark.implicits._
    val lines = List("hello world", "hello roland").toDS
    val histogram = wordCount(lines)
    histogram.show()
    assert(histogram.count() === 3)
  }
}
