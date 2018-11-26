package execution.workers.storage

import io.DiskIOSupport

class ReduceWorkerStorage extends OutputStorage with DiskIOSupport {
  override def write(fileName: String, content: Seq[String]): Unit = {
    ???
  }
}
