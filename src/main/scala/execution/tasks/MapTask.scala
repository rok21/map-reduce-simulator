package execution.tasks

import datastructures.{Dataset, JobSpec, Row}
import datastructures.JobSpec.{KeyVal, MapFunc}
import io.DiskIOSupport
import execution.workers.Storage

import scala.concurrent.{ExecutionContext, Future}

class MapTask(
  inputFileUri: String,
  mapFunc: MapFunc,
  numberOfOutputPartitions: Int,
  outputStorage: Storage)(implicit ec: ExecutionContext) extends DiskIOSupport {

  def execute() = Future {
    val dataset = Dataset.fromCsv(readFile(inputFileUri))
    val mapped: Seq[JobSpec.KeyVal] = mapFunc(dataset)
    val partitioned = mapped
        .groupBy { case KeyVal(key, value) =>
          key.hashCode() % numberOfOutputPartitions
        }

    val fileNames = partitioned.map {
      case (partition, pairs) =>
        val fileName = s"intermediate-${inputFileUri.hashCode()}-$partition.csv"
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
