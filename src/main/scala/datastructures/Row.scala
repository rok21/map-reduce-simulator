package datastructures

case class Row(data: Map[String, String]) {

  def merge(keyVal: (String, String)): Row = keyVal match {
    case (key, value) => new Row(data.updated(key, value))
  }

  def merge(other: Row): Row = {
    other.keys.foldLeft(this) { (row, key) =>
      val updatedRow: Row = row.merge((key, other(key)))
      updatedRow
    }
  }

  def apply(key: String) = data(key)

  def keys = data.keys
}

object Row {
  def empty = new Row(Map.empty)

  def apply(a: (String, String)*) = new Row(a.toMap)

  val intermediateKeyColumnName = "@@intermediateKeyColumnName@@"
}
