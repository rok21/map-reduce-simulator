package execution.workers

import akka.pattern.pipe
import execution.tasks.MapTask
import execution.workers.WorkerActor.TaskCompleted
import execution.workers.storage.MapWorkerStorage


class MapWorker extends WorkerActor {
  import execution.workers.MapWorker._

  override def receive: Receive = idle orElse busy

  val storage = new MapWorkerStorage()

  def handleFileAccess: Receive = {
    case ReadFile(fileName) => sender() ! storage.read(fileName)
  }

  def busy: Receive = handleStateCheck orElse handleFileAccess

  def idle: Receive = {
    case ExecuteTask(mapTask) =>
      context.become(busy)
      val future = mapTask.execute(storage).map(TaskCompleted) pipeTo sender()
      future.foreach(_ => context.become(idle))
  }
}

object MapWorker {
  case class ExecuteTask(mapTask: MapTask)
  case class ReadFile(fileName: String)
}
