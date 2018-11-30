package execution

import scala.concurrent.{ExecutionContext, Future}

trait ExecutionStopwatchSupport {
  def timedInMs[T](task: () => Future[T])(implicit ec: ExecutionContext) =
  {
    val start = System.currentTimeMillis()
    task().map{
      result =>
        val timeElapsed = System.currentTimeMillis() - start
        (result, timeElapsed)
    }
  }
}
