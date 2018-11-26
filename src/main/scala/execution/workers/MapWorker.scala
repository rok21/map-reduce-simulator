package execution.workers

import akka.pattern.pipe
import execution.tasks.MapTask
import execution.workers.WorkerActor._
import execution.workers.storage.MapWorkerStorage


class MapWorker extends WorkerActor {
  import execution.workers.MapWorker._

  override def receive: Receive = idle

  val storage = new MapWorkerStorage()
  def busy: Receive = handleStateCheck orElse handleFileAccess

  def idle: Receive = handleWork orElse busy

  def handleWork : Receive = {
    case ExecuteTask(mapTask) =>
      val start = System.currentTimeMillis()
      val future = mapTask.execute(storage)
      becomeBusy
      future.foreach { files =>
        println(s"Map task completed in ${calcElapsed(start)} ms. Intermediate files produced: ${files.mkString(",")}")
        becomeIdle
      }
      future.map(TaskCompleted) pipeTo sender()
  }

  def handleFileAccess: Receive = {
    case GetFile(fileName) =>
      sender() ! storage.read(fileName)
  }

  private def becomeBusy = {
    currentState = Busy
    context.become(busy)
  }

  private def becomeIdle = {
    currentState = Idle
    context.become(idle)
  }
}

object MapWorker {
  case class ExecuteTask(mapTask: MapTask)
  case class GetFile(fileName: String)
}
