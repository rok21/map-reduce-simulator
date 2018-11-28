package execution.tasks

object Task {

  trait TaskState

  case object Idle extends TaskState

  case object InProgress extends TaskState

  case object Completed extends TaskState

}
