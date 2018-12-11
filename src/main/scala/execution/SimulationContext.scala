package execution

import akka.actor.{ActorSystem, Props}
import akka.pattern.{AskTimeoutException, ask}
import akka.util.Timeout
import datastructures.JobSpec.MapReduce
import execution.workers.Master.StartJob
import execution.workers.{MapWorker, Master, ReduceWorker}

import scala.concurrent.duration.FiniteDuration

class SimulationContext {
  val system = ActorSystem("MapReduceSimulation")
  implicit val ec = system.dispatcher
  def createActors(props: Props, numberOfActors: Int) =
    (0 until numberOfActors) map { _ =>
      system.actorOf(props)
    }

  /*
  M - number of map workers
  R - number of reduce workers
  */
  def executeJob(jobSpec: MapReduce, M: Int, R: Int, maxDuration: FiniteDuration) = {
    implicit val timeout = new Timeout(maxDuration)
    val mapWorkers = createActors(Props(new MapWorker()), M)
    val reduceWorkers = createActors(Props(new ReduceWorker(jobSpec.outputDir)), R)
    val master = system.actorOf(Props(new Master(jobSpec, mapWorkers, reduceWorkers)))
    (master ? StartJob) map { case Master.JobCompleted(outputFiles) =>
        println(s"MapReduce job completed successfully. Final output files produced:")
        outputFiles.foreach(println)
    } recover { case e: AskTimeoutException =>
      println(s"MapReduce job didn't complete in $maxDuration")
    } foreach  { _ =>
      system.terminate()
    }

  }

}