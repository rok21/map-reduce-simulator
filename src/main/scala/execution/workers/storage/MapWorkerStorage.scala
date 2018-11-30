package execution.workers.storage

import java.util.UUID

import io.DiskIOSupport

import scala.collection.mutable

/*
MapReduce framework stores intermediate results of map workers on their local disks (local write),
while reduce workers access them remotely (remote read).

In this simulation, the files are stored on the disk, but the mapping between logical and physical files is local
to map worker's actor.
 */
class MapWorkerStorage extends DiskIOSupport {
  private val fileMap = mutable.Map[String, String]()

  def write(fileName: String, content: Seq[String]) = {
    val physicalFileName = s"intermediate/${UUID.randomUUID()}"
    fileMap.update(fileName, physicalFileName)
    writeFile(physicalFileName, content)
  }

  def read(fileName: String) =
    readFile(fileMap(fileName))
}
