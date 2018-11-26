package execution.workers

import akka.actor.Actor


trait WorkerActor extends Actor {

  import execution.workers.WorkerActor._

  implicit val ec = context.dispatcher

  var currentState: WorkerState = Idle

  def handleStateCheck: Receive = {
    case GetState => sender() ! currentState
  }

  def calcElapsed(start: Long) = System.currentTimeMillis() - start
}
object WorkerActor {

  case object GetState

  trait WorkerState
  case object Idle extends WorkerState
  case object Busy extends WorkerState

  case class TaskCompleted(outputFiles: Seq[String])
}
