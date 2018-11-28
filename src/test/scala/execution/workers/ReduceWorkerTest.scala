package execution.workers

import java.io.File

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import datastructures.JobSpec.{DataForKey, ReduceFunc}
import datastructures.{Dataset, Row}
import execution.workers.Master.RemoteFileAddress
import execution.tasks.ReduceTask
import execution.workers.MapWorker.GetFile
import execution.workers.ReduceWorker.ExecuteTask
import execution.workers.WorkerActor.TaskCompleted
import io.DiskIOSupport
import org.scalatest.{FunSuiteLike, Matchers}

import scala.concurrent.duration._

class ReduceWorkerTest extends TestKit(ActorSystem("MapWorkerTest")) with FunSuiteLike with Matchers with ImplicitSender with DiskIOSupport{
  import ReduceWorkerTest._

  implicit val ec = system.dispatcher

  val nodeProbe0 = TestProbe()
  val nodeProbe1 = TestProbe()

  val reduceFunc: ReduceFunc = {
    case (key, values) =>
      Dataset(
        "country" -> key,
        "userCount" -> values.count.toString
      )
  }

  test("download files, execute reduce tasks, write results to disk and respond with a file name") {
    val worker = system.actorOf(Props(new ReduceWorker(0, "output")))

    val remoteFiles = Seq(
      RemoteFileAddress(nodeProbe0.ref, "intermediate-0.csv"),
      RemoteFileAddress(nodeProbe1.ref, "intermediate-0.csv")
    )
    val execTask = ExecuteTask(reduceFunc, remoteFiles)
    worker ! execTask

    nodeProbe0.expectMsg(GetFile("intermediate-0.csv"))
    nodeProbe0.reply(intermediateFile0)
    nodeProbe1.expectMsg(GetFile("intermediate-0.csv"))
    nodeProbe1.reply(intermediateFile1)

    receiveOne(5 seconds) match {
      case TaskCompleted(Seq(outputFile)) =>
        outputFile shouldEqual "output/part-001.csv"
    }

    val finalOutput = Dataset.fromCsv(readFile("output/part-001.csv"))
    finalOutput.count shouldEqual 2
    val firstRow = finalOutput.data.toList(0)
    firstRow("country") shouldEqual "LT"
    firstRow("userCount") shouldEqual "4"
    val secondRow = finalOutput.data.toList(1)
    secondRow("country") shouldEqual "LV"
    secondRow("userCount") shouldEqual "2"

    new File("output/part-001.csv").delete()
  }

  test("respond with correct status throughout the execution cycle") {
    val worker = system.actorOf(Props(new ReduceWorker(0, "output")))

    val remoteFiles = Seq(
      RemoteFileAddress(nodeProbe0.ref, "intermediate-0.csv"),
      RemoteFileAddress(nodeProbe1.ref, "intermediate-0.csv")
    )

    val reduceFuncSlow: ReduceFunc = {
      case (key, values) =>
        Thread.sleep(1000)
        Dataset(
          "country" -> key,
          "userCount" -> values.count.toString
        )
    }

    val execTask = ExecuteTask(reduceFuncSlow, remoteFiles)

    worker ! WorkerActor.GetState
    expectMsg(WorkerActor.Idle)

    worker ! execTask

    worker ! WorkerActor.GetState
    expectMsg(WorkerActor.Busy)

    nodeProbe0.expectMsg(GetFile("intermediate-0.csv"))
    nodeProbe0.reply(intermediateFile0)
    nodeProbe1.expectMsg(GetFile("intermediate-0.csv"))
    nodeProbe1.reply(intermediateFile1)

    receiveOne(4 seconds).isInstanceOf[TaskCompleted] shouldEqual true
    worker ! WorkerActor.GetState
    expectMsg(WorkerActor.Idle)
  }
}

object ReduceWorkerTest {
  val intermediateFile0 = Seq(
    s"${Row.intermediateKeyColumnName},id,country",
    s"LT,100,LT",
    s"LV,101,LV",
    s"LV,102,LV"
  )
  val intermediateFile1 = Seq(
    s"${Row.intermediateKeyColumnName},id,country",
    s"LT,106,LT",
    s"LT,107,LT",
    s"LT,108,LT"
  )
}
