package execution.workers

import akka.actor.{Actor, ActorRef}
import akka.util.Timeout
import datastructures.JobSpec.MapReduce
import execution.tasks.{MapTask, Task}
import execution.tasks.Task.TaskState
import execution.workers.WorkerActor.WorkerState
import io.DiskIOSupport
import akka.pattern.ask
import scala.concurrent.duration._

class Master(mapWorkers: Seq[ActorRef], reduceWorkers: Seq[ActorRef]) extends WorkerActor with DiskIOSupport {
  import execution.workers.Master._
  var mapTasks = Map[MapTask, ActiveMapTask]()
  var mapTasksInProgress = Map[ActorRef, MapTask]()
  var workerStates = Map[ActorRef, WorkerState]()

  override def busy: Receive = handleStateCheck

  override def idle: Receive = handleWork orElse busy

  def handleWork() : Receive = {
    case jobSpec: MapReduce =>
      becomeBusy
      mapTasks = initializeMapTasks(jobSpec)
      workerStates = initializeWorkerStates
      scheduleMapTasks
      schedulePeriodicalChecks
  }

  def handleTaskChecks: Receive = {
    case CheckWorker(worker) =>

      mapTasks(mapTask).worker.map {
        case worker =>
          implicit val checkTimeout = Timeout(1 second)
          (worker ? WorkerActor.GetState).map {
            case WorkerActor.Busy
          }
      }
  }

  private def scheduleMapTasks = {
    val idleWorkers = workerStates.collect {
      case (worker, WorkerActor.Idle) => worker
    }
    val idleTasks = mapTasks.collect {
      case (task, ActiveMapTask(Task.Idle, _, _)) => task
    }
    idleWorkers.zip(idleTasks) foreach {
      case (worker, task) =>
        mapTasks = mapTasks.updated(task, ActiveMapTask(Task.InProgress, Some(worker), Nil))
        workerStates = workerStates.updated(worker, WorkerActor.Busy)
        worker ! MapWorker.ExecuteTask(task)
    }
  }

  private def schedulePeriodicalChecks = {
    (mapWorkers ++ reduceWorkers) foreach { worker =>
      context.system.scheduler.schedule(0 seconds, 2 second, self, CheckWorker(worker))
    }
  }

  private def initializeMapTasks(jobSpec: MapReduce) = {
    val filesAndFuncs = for {
      mapSpec <- jobSpec.map
      inputFile <- lsDir(mapSpec.inputDir)
    } yield (inputFile, mapSpec.mapFunc)

    val tasks = filesAndFuncs map {
      case (inputFile, mapFunc) => MapTask(inputFile, mapFunc, reduceWorkers.size)
    }

    tasks.map { task =>
      (task, ActiveMapTask(Task.Idle, None, Nil))
    }.toMap
  }

  private def initializeWorkerStates =
    (mapWorkers ++ reduceWorkers).map(worker => (worker, WorkerActor.Idle)).toMap

  private def mapStageCompleted = {
    mapTasks.count { case (_, ActiveMapTask(state, _, _)) => state != Task.Completed } == 0
  }
}

object Master {
  case class RemoteFileAddress(node: ActorRef, fileName: String)
  case class ActiveMapTask(
    state: TaskState,
    worker: Option[ActorRef],
    producedFiles: Seq[String]
  )

  case class CheckWorker(actorRef: ActorRef)
}