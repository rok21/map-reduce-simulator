package execution.workers

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
        dataRows <- Future.sequence(readFutures).map(_.flatten)
        dataset = Dataset.fromCsv(dataRows)
        grouped = dataset.sortAndGroupByIntermediateKey
        keyFuture = for {
          dataForKey <- grouped
          reduceTask = new ReduceTask(dataForKey, reduceFunc)
        } yield reduceTask.execute()
        partitionFuture <- Future.sequence(keyFuture)
      } yield partitionFuture

      datasetsF.map { datasets =>
        val csv = toCsv(datasets)
        write(outputDir, partitionId, csv)
      }.map { fileWritten =>
        println(s"Reduce task completed in ${calcElapsed(start)} ms. Output file produced: $fileWritten")
        becomeIdle
        sender() ! TaskCompleted(Seq(fileWritten))
      }
  }



  private def toCsv(datasets: Seq[Dataset]) =
    Dataset.toCsvRows(new Dataset(datasets.flatMap(_.data)))
}

object ReduceWorker {
  case class ExecuteTask(reduceFunc: ReduceFunc, remoteFiles: Seq[RemoteFileAddress])
}
