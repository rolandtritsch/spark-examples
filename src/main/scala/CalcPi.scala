package org.tritsch.spark.examples

import org.apache.spark.sql._

object CalcPi {
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Example: CalcPi")
    .getOrCreate()

  def calcPi(): Double = {
    val n = 10000000
    val c = spark.sparkContext.parallelize(1 to n).filter { _ =>
      val x = math.random
      val y = math.random
      x*x + y*y < 1
    }.count()
    4.0 * c / n
  }

  def main(args: Array[String]): Unit = {
    require(args.size == 0, "Usage: CalcPi")

    println(s"${calcPi()}")

    spark.stop()
  }
}
