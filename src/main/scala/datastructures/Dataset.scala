package datastructures

import datastructures.JobSpec.{DataForKey, Dataset}


object Dataset {

  def fromCsv(contentRows: Seq[String]): Dataset = contentRows.headOption match {
    case Some(headerRow) =>
      val headers = headerRow.split(",")
      contentRows.tail.map { str =>
        val values = str.split(",")
        headers.zip(values).toMap
      }
    case None => Seq.empty
  }

  def toCsvRows(dataset: Dataset) : Seq[String] = {
    val columnsUnion = dataset.foldLeft(Seq.empty[String]){ case (union, currentRow) =>
      union.union(currentRow.keys.toSeq).distinct
    }
    columnsUnion match {
      case Nil => Seq.empty
      case _ =>
        val headerRow = columnsUnion.mkString(",")

        val bodyRows = dataset.map { row =>
          columnsUnion.map { col =>
            row.getOrElse(key = col, default = "")
          }.mkString(",")
        }
        headerRow +: bodyRows
    }
  }

  def sortAndGroupByIntermediateKey(dataset: Dataset): Seq[DataForKey] =
    dataset.groupBy(row => row(intermediateKeyColumnName))
        .map {
          case (key, intermediateRows) =>
            //remove intermediate key from the dataset
            (key, intermediateRows.map(ir => ir - intermediateKeyColumnName))
        }
        .toSeq.sortBy { case (key, _) => key }

  val intermediateKeyColumnName = "@@intermediateKeyColumnName@@"
}
