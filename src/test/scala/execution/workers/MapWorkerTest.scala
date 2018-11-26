package execution.workers

import akka.actor.{ActorSystem, Props}
import datastructures.JobSpec.{KeyVal, MapFunc}
import execution.tasks.MapTask
import org.scalatest.{Matchers, WordSpec}

class MapWorkerTest extends WordSpec with Matchers {

  val sys = ActorSystem("MapWorkerTest")

  val actor = sys.actorOf(Props(new MapWorker))
  implicit val ec = sys.dispatcher

  val mapFunc: MapFunc =
    users => users.map { user =>
      Thread.sleep(100)
      KeyVal(
        key = user("country"),
        value = user
      )
    }

  val task = new MapTask(
    "src/test/resources/data/users/part-001.csv",
    mapFunc = mapFunc,
    numberOfOutputPartitions = 2
  )

  "execute a task and send back file names" in {

      ???
    //task.
  }


}
