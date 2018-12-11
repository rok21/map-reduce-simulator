package execution.workers

import akka.actor.Actor
import datastructures.Dataset
import datastructures.JobSpec.ReduceFunc
import io.DiskIOSupport

class ReduceWorker(partition: Int, reduceFunc: ReduceFunc, outputDir: String, mapTasksCount: Int) extends Actor with DiskIOSupport {

  implicit val ec = context.dispatcher
  var intermediateFiles = Seq.empty[Option[String]]

  def receive: Receive = {
    case intermediateFile: Option[String] =>
      intermediateFiles = intermediateFiles :+ intermediateFile
      if(intermediateFiles.size == mapTasksCount) //map stage finished
        execTask
  }

  private def execTask = {
    val fileContents = intermediateFiles.flatten.map(readFile)
    val dataset = fileContents.flatMap(Dataset.fromCsv)
    val grouped = Dataset.sortAndGroupByIntermediateKey(dataset)
    val finalDataset = grouped flatMap reduceFunc
    val partitionSuffix: String = (1000 + partition + 1).toString.tail
    val fileName = s"$outputDir/part-$partitionSuffix.csv"
    writeFile(fileName, Dataset.toCsvRows(finalDataset))
    println(s"Reduce task completed successfully. File produced: $fileName")
  }
}