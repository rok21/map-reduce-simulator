package execution.tasks

import execution.workers.storage.OutputStorage

import scala.concurrent.{ExecutionContext, Future}

trait Task {

  def execute(outputStorage: OutputStorage)(implicit ec: ExecutionContext): Future[Seq[String]]

}
