package org.tritsch.spark.examples

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpec

class CalcPiSpec extends FlatSpec with BeforeAndAfterAll {
  import CalcPi._

  override def afterAll(): Unit = {
    spark.close()
  }

  "calcPi()" should "return an approximation of PI" in {
    val pi = math.floor(calcPi() * 100) / 100
    assert(pi == 3.14)
  }
}
