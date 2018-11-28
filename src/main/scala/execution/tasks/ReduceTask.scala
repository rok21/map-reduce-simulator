package execution.tasks
import datastructures.JobSpec.{DataForKey, ReduceFunc}

import scala.concurrent.{ExecutionContext, Future}

case class ReduceTask(
  data: DataForKey,
  reduceFunc: ReduceFunc
) {
  def execute()(implicit ec: ExecutionContext) = Future { reduceFunc(data) }
}
