package execution.workers.storage

import akka.pattern.ask
import akka.actor.ActorRef
import akka.util.Timeout
import datastructures.Dataset
import execution.Master.RemoteFileAddress
import execution.workers.MapWorker
import io.DiskIOSupport

import scala.concurrent.duration._

trait ReduceWorkerStorage extends DiskIOSupport {

  def write(outputDir: String, partitionNumber: Int, content: Seq[String]) = {
    val fileName = getFileName(outputDir, partitionNumber)
    writeFile(fileName, content)
    fileName
  }

  implicit val remoteFileAccessTimeout = new Timeout(3 seconds)
  def remoteRead(remoteFileAddress: RemoteFileAddress) =
    (remoteFileAddress.node ? MapWorker.GetFile(remoteFileAddress.fileName)).mapTo[Seq[String]]

  def getFileName(outputDir: String, partitionNumber: Int) = {
    val partitionSuffix: String = (1000 + partitionNumber + 1).toString.tail
    s"$outputDir/part-$partitionSuffix.csv"
  }
}
