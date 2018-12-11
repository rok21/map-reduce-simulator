package execution.workers

import akka.actor.{Actor, ActorRef}
import datastructures.JobSpec.MapReduce
import execution.tasks.MapTask
import io.DiskIOSupport

class Master(jobSpec: MapReduce, mapWorkers: Seq[ActorRef], reduceWorkers: Seq[ActorRef]) extends Actor with DiskIOSupport {
  import execution.workers.Master._

  var idleMapTasks = Seq[MapTask]()
  var idleMapWorkers = mapWorkers
  val partitionCount = reduceWorkers.size
  var intermediateFiles: Map[Int, Seq[String]] = 0 until partitionCount map (p => p -> Seq.empty[String]) toMap
  var finalOutputFiles = Seq.empty[String]

  def idle: Receive = {
    case StartJob =>
      idleMapTasks = initializeMapTasks
      scheduleMapTasks
      checkForMapStageCompletion
      context.become(busy(sender()))
  }

  def busy(replyTo: ActorRef): Receive = {
    case StartJob =>
      idleMapTasks = initializeMapTasks
      scheduleMapTasks
      checkForMapStageCompletion

    case MapWorker.TaskCompleted(result) =>
      val mapWorker = sender()
      idleMapWorkers = idleMapWorkers :+ mapWorker
      scheduleMapTasks
      rememberIntermediateFiles(mapWorker, result)
      checkForMapStageCompletion

    case MapStageCompleted =>
      scheduleReduceTasks
      checkForReduceStageCompletion

    case ReduceWorker.TaskCompleted(outputFile) =>
      finalOutputFiles = finalOutputFiles :+ outputFile
      checkForReduceStageCompletion

    case ReduceStageCompleted =>
      replyTo ! JobCompleted(finalOutputFiles)
      context.become(idle)
  }

  def receive : Receive = idle


  private def scheduleReduceTasks = (reduceWorkers zip intermediateFiles) foreach {
    case (worker, (partition, intermediateFiles)) =>
      worker ! ReduceWorker.ExecuteTask(reduceFunc = jobSpec.reduce.reduceFunc, intermediateFiles, partition)
  }

  private def rememberIntermediateFiles(mapWorker: ActorRef, result: Map[Int, Option[String]]) =
    result foreach {
      case (partitionId, Some(fileName)) =>
        intermediateFiles = intermediateFiles.updated(partitionId, intermediateFiles(partitionId) :+ fileName)
      case _ => ()
    }

  private def scheduleMapTasks = {
    val pairs = idleMapWorkers.zip(idleMapTasks)
    pairs foreach {
      case (worker, task) =>
        idleMapTasks = idleMapTasks.tail
        idleMapWorkers = idleMapWorkers.tail
        worker ! MapWorker.ExecuteTask(task)
    }
  }

  private def initializeMapTasks = {
    val filesAndFuncs = for {
      mapSpec <- jobSpec.map
      inputFile <- lsDir(mapSpec.inputDir)
    } yield (inputFile, mapSpec.mapFunc)

   filesAndFuncs map {
      case (inputFile, mapFunc) => MapTask(inputFile, mapFunc, reduceWorkers.size)
    }
  }

  private def checkForMapStageCompletion =
    if(idleMapTasks.isEmpty && idleMapWorkers.size == mapWorkers.size) {
      self ! MapStageCompleted
    }

  private def checkForReduceStageCompletion =
    if(finalOutputFiles.size == partitionCount) {
      self ! ReduceStageCompleted
    }
}

object Master {
  case object StartJob
  case class JobCompleted(outputFiles: Seq[String])

  private case object MapStageCompleted
  private case object ReduceStageCompleted
}