package execution.tasks

import datastructures.JobSpec.{KeyVal, MapFunc}
import datastructures.{Dataset, JobSpec, Row}
import execution.workers.storage.MapWorkerStorage
import io.DiskIOSupport

import scala.concurrent.{ExecutionContext, Future}

case class MapTask(
  inputFileName: String,
  mapFunc: MapFunc,
  numberOfOutputPartitions: Int) extends DiskIOSupport {

  def execute(outputStorage: MapWorkerStorage)(implicit ec: ExecutionContext): Future[Seq[String]] = Future {
    val dataset = Dataset.fromCsv(readFile(inputFileName))
    val mapped: Seq[JobSpec.KeyVal] = mapFunc(dataset)
    val partitioned = mapped
        .groupBy { case KeyVal(key, _) =>
          key.hashCode() % numberOfOutputPartitions
        }

    val fileNames = partitioned.map {
      case (partition, pairs) =>
        val fileName = s"intermediate-${inputFileName.hashCode()}-$partition.csv"
        val intermediateDataset = new Dataset(
          pairs.map {
            case KeyVal(key, row) => row.merge(Row.intermediateKeyColumnName, key)
          }
        )
        outputStorage.write(fileName, Dataset.toCsvRows(intermediateDataset))
        fileName
    }

    fileNames.toSeq
  }
}
