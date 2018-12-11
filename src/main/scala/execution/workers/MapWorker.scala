package execution.workers

import java.util.UUID

import akka.actor.{Actor, ActorRef}
import datastructures.JobSpec.{KeyVal, MapFunc}
import datastructures.{Dataset, JobSpec}
import execution.workers.MapWorker.MapTask
import io.DiskIOSupport


class MapWorker(reduceWorkers: List[ActorRef]) extends Actor with DiskIOSupport {
  implicit val ec = context.dispatcher
  private val numberOfPartitions = reduceWorkers.size

  def receive: Receive = {
    case task: MapTask =>
      val result = execute(task)
      val files = result.flatMap(_._2)
      println(s"Map task completed successfully. Intermediate files produced: ${files.mkString(",")}")
      result.foreach { case (partition, file) =>
        reduceWorkers(partition) ! file
      }
  }

  private def execute(task: MapTask): Map[Int, Option[String]] = {
    val dataset = Dataset.fromCsv(readFile(task.inputFile))
    val mapped: Seq[JobSpec.KeyVal] = task.mapFunc(dataset)

    val partitioned = mapped
        .groupBy { case KeyVal(key, _) =>
          Math.abs(key.hashCode() % numberOfPartitions)
        }

    val emptyMap: Map[Int, Option[String]] = 0 until numberOfPartitions map { i => i -> None  } toMap

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

object MapWorker {
  case class MapTask(inputFile: String, mapFunc: MapFunc)
}