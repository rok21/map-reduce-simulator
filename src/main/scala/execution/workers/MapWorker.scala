package execution.workers

import akka.actor.Actor
import akka.pattern.pipe
import execution.tasks.MapTask


class MapWorker extends Actor {
  import execution.workers.MapWorker._
  implicit val ec = context.dispatcher
  def receive: Receive = {
    case ExecuteTask(mapTask) =>
      mapTask.execute map { result =>
          val files = result.flatMap(_._2)
          println(s"Map task completed successfully. Intermediate files produced: ${files.mkString(",")}")
          TaskCompleted(result)
      } pipeTo sender()
  }
}

object MapWorker {
  case class ExecuteTask(mapTask: MapTask)
  case class TaskCompleted(taskResult: Map[Int, Option[String]])
}
