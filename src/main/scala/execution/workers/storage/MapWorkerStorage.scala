package execution.workers.storage

import java.util.UUID

import io.DiskIOSupport

import scala.collection.mutable

/*
In the actual mapreduce framework intermediate results of map workers are stored on their local disks (local write),
while reduce workers access them remotely (remote read). In an event of a map worker failure
these intermediate results are lost which means that even completed map tasks need to be re-executed.

To simulate this scenario we use mapping between logical and physical intermediate files.

The files are stored on the disk, but the mapping is local to map worker's actor,
making it necessary for the map worker to be up in order to access its intermediate files.

 */
class MapWorkerStorage extends OutputStorage with DiskIOSupport {
  private val fileMap = mutable.Map[String, String]()

  override def write(fileName: String, content: Seq[String]) = {
    val physicalFileName = s"intermediate/${UUID.randomUUID()}"
    fileMap.update(fileName, physicalFileName)
    writeFile(physicalFileName, content)
  }

  def read(fileName: String) =
    readFile(fileMap(fileName))
}