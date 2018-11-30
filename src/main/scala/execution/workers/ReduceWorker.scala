package execution.workers

import akka.pattern.pipe
import datastructures.Dataset
import datastructures.JobSpec.ReduceFunc
import execution.workers.Master.RemoteFileAddress
import execution.tasks.ReduceTask
import execution.workers.storage.ReduceWorkerStorage

import scala.concurrent.Future

class ReduceWorker(outputDir: String) extends WorkerActor with ReduceWorkerStorage {

  import execution.workers.ReduceWorker._

  def receive: Receive = handleWork

  def handleWork: Receive = {
    case task: ExecuteTask =>
      timedInMs {
        () => execTask(task)
      } map { case (fileWritten, elapsedMs) =>
        println(s"Reduce task completed in $elapsedMs ms. Output file produced: $fileWritten")
        TaskCompleted(fileWritten)
      } pipeTo sender()
  }

  private def execTask(task: ExecuteTask) = {
    val readFutures = task.remoteFiles.map(remoteRead)
    val datasetsF = for {
      files <- Future.sequence(readFutures)
      datasetsPerPartition = files.map(Dataset.fromCsv)
      mainDataset = Dataset.merge(datasetsPerPartition)
      grouped = mainDataset.sortAndGroupByIntermediateKey
      keyFuture = for {
        dataForKey <- grouped
        reduceTask = new ReduceTask(dataForKey, task.reduceFunc)
      } yield reduceTask.execute()
      partitionFuture <- Future.sequence(keyFuture)
    } yield partitionFuture

    datasetsF.map { datasetsPerKey =>
      val csv = Dataset.toCsvRows(Dataset.merge(datasetsPerKey))
      write(outputDir, task.partitionId, csv)
    }
  }

}

object ReduceWorker {
  case class ExecuteTask(reduceFunc: ReduceFunc, remoteFiles: Seq[RemoteFileAddress], partitionId: Int)
  case class TaskCompleted(file: String)
}
