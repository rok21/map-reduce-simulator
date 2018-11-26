package execution.workers.storage

trait OutputStorage {
  def write(fileName: String, content: Seq[String]): Unit
}
