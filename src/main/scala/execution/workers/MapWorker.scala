package execution.workers

import akka.pattern.pipe
import execution.tasks.MapTask
import execution.tasks.MapTask.MapTaskResult
import execution.workers.storage.MapWorkerStorage


class MapWorker extends WorkerActor {
  import execution.workers.MapWorker._

  val storage = new MapWorkerStorage()

  def receive: Receive = handleWork orElse handleFileAccess

  def handleWork : Receive = {
    case ExecuteTask(mapTask) =>
      timedInMs {
        () => mapTask.execute(storage)
      } map {
        case (result, elapsedMs) =>
          val files = result.partitions.flatMap(_._2)
          println(s"Map task completed in $elapsedMs ms. " +
                      s"Intermediate files produced: ${files.mkString(",")}")
          TaskCompleted(result)
      } pipeTo sender()
  }

  def handleFileAccess: Receive = {
    case GetFile(fileName) =>
      sender() ! storage.read(fileName)
  }
}

object MapWorker {
  case class ExecuteTask(mapTask: MapTask)
  case class GetFile(fileName: String)
  case class TaskCompleted(taskResult: MapTaskResult)
}
