package execution

import akka.actor.{ActorSystem, Props}
import akka.routing.RoundRobinPool
import akka.util.Timeout
import datastructures.JobSpec.MapReduce
import execution.workers.MapWorker.MapTask
import execution.workers.{MapWorker, ReduceWorker}
import io.DiskIOSupport

import scala.concurrent.duration.FiniteDuration

class SimulationContext extends DiskIOSupport{
  val system = ActorSystem("MapReduceSimulation")
  implicit val ec = system.dispatcher

  /*
  M - number of map workers
  R - number of reduce workers
  */
  def executeJob(jobSpec: MapReduce, M: Int, R: Int) = {
    val mapTasks = for {
      mapSpec <- jobSpec.map
      inputFile <- lsDir(mapSpec.inputDir)
      task = MapTask(inputFile, mapSpec.mapFunc)
    } yield task

    val reduceWorkers = 0 until R map { p =>
      val props = Props(new ReduceWorker(p, jobSpec.reduce.reduceFunc, jobSpec.outputDir, mapTasks.size))
      system.actorOf(props)
    } toList

    val mapWorkersRouter = system.actorOf(RoundRobinPool(M).props(Props(new MapWorker(reduceWorkers))))

    mapTasks foreach { task => mapWorkersRouter ! task }
  }
}