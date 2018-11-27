package execution.workers

import akka.pattern.pipe
import datastructures.Dataset
import datastructures.JobSpec.ReduceFunc
import execution.Master.RemoteFileAddress
import execution.tasks.ReduceTask
import execution.workers.ReduceWorker.ExecuteTask
import execution.workers.WorkerActor.TaskCompleted
import execution.workers.storage.ReduceWorkerStorage

import scala.concurrent.Future

class ReduceWorker(partitionId: Int, outputDir: String) extends WorkerActor with ReduceWorkerStorage {

  def busy: Receive = handleStateCheck

  def idle: Receive = handleWork orElse busy

  def handleWork: Receive = {
    case ExecuteTask(reduceFunc, remoteFiles) =>
      becomeBusy
      val start = System.currentTimeMillis()
      val readFutures = remoteFiles.map(remoteRead)
      val datasetsF = for {
        files <- Future.sequence(readFutures)
        datasetsPerPartition = files.map(Dataset.fromCsv)
        mainDataset = Dataset.merge(datasetsPerPartition)
        grouped = mainDataset.sortAndGroupByIntermediateKey
        keyFuture = for {
          dataForKey <- grouped
          reduceTask = new ReduceTask(dataForKey, reduceFunc)
        } yield reduceTask.execute()
        partitionFuture <- Future.sequence(keyFuture)
      } yield partitionFuture

      datasetsF.map { datasetsPerKey =>
        val csv = Dataset.toCsvRows(Dataset.merge(datasetsPerKey))
        write(outputDir, partitionId, csv)
      }.map { fileWritten =>
        println(s"Reduce task completed in ${calcElapsed(start)} ms. Output file produced: $fileWritten")
        becomeIdle
        TaskCompleted(Seq(fileWritten))
      } pipeTo sender()
  }

}

object ReduceWorker {
  case class ExecuteTask(reduceFunc: ReduceFunc, remoteFiles: Seq[RemoteFileAddress])
}
