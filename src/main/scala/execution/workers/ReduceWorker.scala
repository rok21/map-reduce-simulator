package execution.workers

import akka.actor.Actor
import akka.pattern.pipe
import datastructures.Dataset
import datastructures.JobSpec.ReduceFunc
import execution.tasks.ReduceTask
import io.DiskIOSupport

import scala.concurrent.Future

class ReduceWorker(outputDir: String) extends Actor with DiskIOSupport {

  import execution.workers.ReduceWorker._
  implicit val ec = context.dispatcher

  def receive: Receive = {
    case task: ExecuteTask =>
      execTask(task) map { fileWritten =>
        println(s"Reduce task completed successfully. Output file produced: $fileWritten")
        TaskCompleted(fileWritten)
      } pipeTo sender()
  }

  private def execTask(task: ExecuteTask) = {
    val files = task.intermediateFiles.map(readFile)
    val datasets = files.map(Dataset.fromCsv)
    val mainDataset = datasets.flatten
    val grouped = Dataset.sortAndGroupByIntermediateKey(mainDataset)
    val keyFuture = for {
      dataForKey <- grouped
      reduceTask = new ReduceTask(dataForKey, task.reduceFunc)
    } yield reduceTask.execute()

    Future.sequence(keyFuture).map { datasetsPerKey =>
      val csv = Dataset.toCsvRows(datasetsPerKey.flatten)
      write(outputDir, task.partitionId, csv)
    }
  }

  private def write(outputDir: String, partitionNumber: Int, content: Seq[String]) = {
    val partitionSuffix: String = (1000 + partitionNumber + 1).toString.tail
    val fileName = s"$outputDir/part-$partitionSuffix.csv"
    writeFile(fileName, content)
    fileName
  }
}

object ReduceWorker {
  case class ExecuteTask(reduceFunc: ReduceFunc, intermediateFiles: Seq[String], partitionId: Int)
  case class TaskCompleted(file: String)
}