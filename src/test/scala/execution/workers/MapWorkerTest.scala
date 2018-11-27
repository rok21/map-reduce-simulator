package execution.workers

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import datastructures.JobSpec.KeyVal
import execution.tasks.MapTask
import execution.workers.WorkerActor.TaskCompleted
import org.scalatest.{FunSuiteLike, Matchers}

import scala.concurrent.duration._

class MapWorkerTest extends TestKit(ActorSystem("MapWorkerTest")) with FunSuiteLike with Matchers with ImplicitSender {
  implicit val ec = system.dispatcher
  implicit val askTimeout = new Timeout(10 seconds)

  val quickTask = new MapTask(
    "src/test/resources/data/users/part-001.csv",
    mapFunc = users => users.map { user =>
      KeyVal(
        key = user("country"),
        value = user
      )
    },
    numberOfOutputPartitions = 1
  )

  test("execute a task, send back file names and handle file access") {
    val worker = system.actorOf(Props(new MapWorker))
    worker ! MapWorker.ExecuteTask(quickTask)
    val intermediateFile = receiveOne(1 second) match {
      case TaskCompleted(fileNames) =>
        fileNames.size shouldEqual 1
        fileNames.head
      case x => sys.error(s"Unexpected response from map worker: $x")
    }

    worker ! MapWorker.GetFile(intermediateFile)
    val csvRows = receiveOne(2 seconds) match {
        case rows: Seq[String] => rows
        case x => sys.error(s"Unexpected response from map worker: $x")
    }
    csvRows.size shouldEqual 7 // 6 users + 1 header row
  }

  val slowTask = new MapTask(
    "src/test/resources/data/users/part-001.csv",
    mapFunc = users => users.map { user =>
      Thread.sleep(100)
      KeyVal(
        key = user("country"),
        value = user
      )
    },
    numberOfOutputPartitions = 1
  )

  test("respond with correct status throughout the execution cycle") {
    val worker = system.actorOf(Props(new MapWorker))
    worker ! WorkerActor.GetState
    expectMsg(WorkerActor.Idle)
    worker ! MapWorker.ExecuteTask(slowTask)
    worker ! WorkerActor.GetState
    expectMsg(WorkerActor.Busy)
    receiveOne(2 seconds).isInstanceOf[TaskCompleted] shouldEqual true
    worker ! WorkerActor.GetState
    expectMsg(WorkerActor.Idle)
  }

  test("provide file system access while busy") {
    val worker = system.actorOf(Props(new MapWorker))
    worker ! MapWorker.ExecuteTask(quickTask)

    val intermediateFile = receiveOne(1 second) match {
      case TaskCompleted(fileNames) =>
        fileNames.size shouldEqual 1
        fileNames.head
      case x => sys.error(s"Unexpected response from map worker: $x")
    }

    worker ! MapWorker.ExecuteTask(slowTask)
    worker ! WorkerActor.GetState
    expectMsg(WorkerActor.Busy)
    worker ! MapWorker.GetFile(intermediateFile)
    receiveOne(1 second).isInstanceOf[Seq[String]] shouldEqual true
  }
}
