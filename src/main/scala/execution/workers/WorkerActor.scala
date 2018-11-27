package execution.workers

import akka.actor.Actor


trait WorkerActor extends Actor {

  import execution.workers.WorkerActor._

  implicit val ec = context.dispatcher

  var currentState: WorkerState = Idle

  def busy: Receive

  def idle: Receive

  override def receive: Receive = idle

  def handleStateCheck: Receive = {
    case GetState => sender() ! currentState
  }

  def becomeBusy = {
    currentState = Busy
    context.become(busy)
  }

  def becomeIdle = {
    currentState = Idle
    context.become(idle)
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
