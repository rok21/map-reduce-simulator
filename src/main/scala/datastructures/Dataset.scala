package datastructures

import datastructures.JobSpec.{DataForKey, KeyVal}

class Dataset(val data: Seq[Row]) {

  def sortAndGroupByIntermediateKey: Seq[DataForKey] =
    data.groupBy(row => row(Row.intermediateKeyColumnName))
        .map {
          case (key, intermediateRows) =>
            //remove intermediate key from the dataset
            val rows = intermediateRows.map(ir => Row(ir.data - Row.intermediateKeyColumnName))
            (key, new Dataset(rows))
        }
        .toSeq.sortBy { case (key, _) => key }

  // syntax sugar

  def select(filterFunc: Row => Boolean) = new Dataset(
    data.filter(filterFunc)
  )

  def mapr(mapFunc: Row => Row): Dataset = new Dataset(
    data.map(mapFunc)
  )

  def map(func: Row => KeyVal): Seq[KeyVal] = data.map(func)

  def first = data.headOption

  def count = data.size
}

object Dataset {

  def apply(a: (String, String)*) = new Dataset(Seq(Row(a:_*)))

  def merge(datasets: Seq[Dataset]) = new Dataset(
    datasets.flatMap(_.data)
  )

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
