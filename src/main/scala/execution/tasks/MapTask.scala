package execution.tasks

import java.util.UUID

import datastructures.JobSpec.{KeyVal, MapFunc}
import datastructures.{Dataset, JobSpec}
import io.DiskIOSupport

import scala.concurrent.{ExecutionContext, Future}

case class MapTask(
  inputFileName: String,
  mapFunc: MapFunc,
  numberOfOutputPartitions: Int) extends DiskIOSupport {

  def execute(implicit ec: ExecutionContext): Future[Map[Int, Option[String]]] = Future {
    val dataset = Dataset.fromCsv(readFile(inputFileName))
    val mapped: Seq[JobSpec.KeyVal] = mapFunc(dataset)

    val partitioned = mapped
        .groupBy { case KeyVal(key, _) =>
          Math.abs(key.hashCode() % numberOfOutputPartitions)
        }

    val emptyMap: Map[Int, Option[String]] = 0 until numberOfOutputPartitions map { i => i -> None  } toMap

    val result = partitioned.foldLeft(emptyMap) {
      case (finalResult, (partition, pairs)) =>
        val fileName = s"intermediate/${UUID.randomUUID()}"
        val intermediateDataset = pairs.map {
            case KeyVal(key, row) => row.updated(Dataset.intermediateKeyColumnName, key)
          }
        writeFile(fileName, Dataset.toCsvRows(intermediateDataset))
        finalResult.updated(partition, Some(fileName))
    }
    result
  }
}