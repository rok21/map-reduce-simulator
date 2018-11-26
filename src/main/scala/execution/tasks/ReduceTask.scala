package execution.tasks
import execution.workers.storage.OutputStorage

import scala.concurrent.{ExecutionContext, Future}

class ReduceTask extends Task {
  override def execute(outputStorage: OutputStorage)
      (implicit ec: ExecutionContext): Future[Seq[String]] = ???
}
