package execution.workers

import akka.actor.Actor
import execution.ExecutionStopwatchSupport

trait WorkerActor extends Actor with ExecutionStopwatchSupport {

  implicit val ec = context.dispatcher

}
