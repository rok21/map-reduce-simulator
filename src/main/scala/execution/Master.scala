package execution

import akka.actor.ActorRef
import datastructures.JobSpec.MapReduce

class Master(config: SimulatorConfig, mapWorkers: Seq[ActorRef], reduceWorkers: Seq[ActorRef]) {

  def execute(jobSpec: MapReduce) = {

  }


}

object Master {
  case class RemoteFileAddress(node: ActorRef, fileName: String)
}