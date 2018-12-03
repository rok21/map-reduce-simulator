package execution.tasks

import datastructures.JobSpec.{KeyVal, MapFunc}
import datastructures.{Dataset, JobSpec, Row}
import execution.tasks.MapTask.MapTaskResult
import execution.workers.storage.MapWorkerStorage
import io.DiskIOSupport

import scala.concurrent.{ExecutionContext, Future}

case class MapTask(
  inputFileName: String,
  mapFunc: MapFunc,
  numberOfOutputPartitions: Int) extends DiskIOSupport {

  def execute(outputStorage: MapWorkerStorage)(implicit ec: ExecutionContext): Future[MapTaskResult] = Future {
    val dataset = Dataset.fromCsv(readFile(inputFileName))
    val mapped: Seq[JobSpec.KeyVal] = mapFunc(dataset)

    val partitioned = mapped
        .groupBy { case KeyVal(key, _) =>
          Math.abs(key.hashCode() % numberOfOutputPartitions)
        }

    val result = partitioned.foldLeft(MapTaskResult(numberOfOutputPartitions)) {
      case (finalResult, (partition, pairs)) =>
        val fileName = s"intermediate-${inputFileName.hashCode()}-$partition.csv"
        val intermediateDataset = new Dataset(
          pairs.map {
            case KeyVal(key, row) => row.merge(Row.intermediateKeyColumnName, key)
          }
        )
        outputStorage.write(fileName, Dataset.toCsvRows(intermediateDataset))
        finalResult.copy(finalResult.partitions.updated(partition, Some(fileName)))
    }

    result
  }
}

object MapTask {
  case class MapTaskResult(partitions: Map[Int, Option[String]])
  object MapTaskResult {
    def apply(partitionCount: Int): MapTaskResult = {
      val map: Map[Int, Option[String]] =
        0 until partitionCount map { i => i -> None  } toMap

      new MapTaskResult(map)
    }
  }
}