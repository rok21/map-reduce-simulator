package datastructures

import org.scalatest.{Matchers, WordSpec}


class RowTest extends WordSpec with Matchers {

  "merge with another Row" in {

    val row0 = Row("col0" -> "a")
    val row1 = Row("col1" -> "b")
    row0.merge(row1) shouldEqual Row("col0" -> "a", "col1" -> "b")
  }


}
