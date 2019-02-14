package org.tritsch.spark.examples

import org.apache.spark.sql._

object WordCount {
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Example: Word Count")
    .getOrCreate()

  def wordCount(lines: Dataset[String]): Dataset[(String, Int)] = {
    import spark.implicits._
    val words = lines.flatMap { l => l.split(" ") }.map { w => (w, 1) }.rdd
    val histogram = words.reduceByKey { (counter, instance) => counter + instance }.toDS
    histogram.sort($"_2".desc)
  }

  def main(args: Array[String]): Unit = {
    require(args.size == 1, "Usage: WordCount <fileName>")

    val fileName = args(0)
    val lines = spark.read.textFile(fileName)

    wordCount(lines).show()

    spark.stop()
  }
}
