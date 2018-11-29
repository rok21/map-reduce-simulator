package execution.workers

import akka.actor.Actor

import scala.concurrent.Future

trait WorkerActor extends Actor {

  implicit val ec = context.dispatcher

  def timedInMs[T](task: () => Future[T]) =
  {
    val start = System.currentTimeMillis()
    task().map{
      result =>
        val timeElapsed = System.currentTimeMillis() - start
        (result, timeElapsed)
    }
  }

}
