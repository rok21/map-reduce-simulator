package execution.workers

import akka.pattern.pipe
import execution.tasks.MapTask
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
        case (files, elapsedMs) =>
          println(s"Map task completed in $elapsedMs ms. Intermediate files produced: ${files.mkString(",")}")
          TaskCompleted(files)
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
  case class TaskCompleted(fileNames: Seq[String])
}
