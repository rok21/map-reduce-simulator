package datastructures

import datastructures.JobSpec.{DataForKey, KeyVal}

class Dataset(val data: Seq[Row]) {

  def select(filterFunc: Row => Boolean) = new Dataset(
    data.filter(filterFunc)
  )

  def map(mapFunc: Row => Row): Dataset = new Dataset(
    data.map(mapFunc)
  )

  def map(func: Row => KeyVal): Seq[KeyVal] = data.map(func)

  def first = data.headOption

  def sortAndGroupByIntermediateKey: Seq[DataForKey] =
    data.sortBy(row => row(Row.intermediateKeyColumnName))
        .groupBy(row => row(Row.intermediateKeyColumnName))
        .map {
          case (key, rows) => DataForKey(key, new Dataset(rows))
        }
        .toSeq
}

object Dataset {
  def fromCsv(contentRows: Seq[String]): Dataset = contentRows.headOption match {
    case Some(headerRow) =>
      val headers = headerRow.split(",")
      val dataRows = contentRows.tail.map { str =>
        val values = str.split(",")
        Row(headers.zip(values) :_*)
      }
      new Dataset(dataRows)
    case None => new Dataset(Seq.empty)
  }

  def toCsvRows(dataset: Dataset) : Seq[String] = {
    dataset.first match {
      case Some(firstRow) =>
        val headerRow = firstRow.keys.mkString(",")
        val dataRows = dataset.data.map {
          row => row.data.values.mkString(",")
        }
        headerRow +: dataRows
      case None => Seq.empty
    }
  }
}
