package execution.workers

import java.io.File

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import datastructures.JobSpec.ReduceFunc
import datastructures.{Dataset, Row}
import execution.workers.MapWorker.GetFile
import execution.workers.Master.RemoteFileAddress
import execution.workers.ReduceWorker.{ExecuteTask, TaskCompleted}
import io.DiskIOSupport
import org.scalatest.{FunSuiteLike, Matchers}

import scala.concurrent.duration._

class ReduceWorkerTest extends TestKit(ActorSystem("MapWorkerTest")) with FunSuiteLike with Matchers with ImplicitSender with DiskIOSupport {
  import ReduceWorkerTest._

  implicit val ec = system.dispatcher

  val nodeProbe0 = TestProbe()
  val nodeProbe1 = TestProbe()

  test("download files, execute reduce tasks, write results to disk and respond with a file name") {
    val worker = system.actorOf(Props(new ReduceWorker("src/test/resources/data/output")))

    val remoteFiles = Seq(
      RemoteFileAddress(nodeProbe0.ref, "intermediate-0.csv"),
      RemoteFileAddress(nodeProbe1.ref, "intermediate-0.csv")
    )
    val execTask = ExecuteTask(reduceFunc, remoteFiles, 0)
    worker ! execTask

    nodeProbe0.expectMsg(GetFile("intermediate-0.csv"))
    nodeProbe0.reply(intermediateFile0)
    nodeProbe1.expectMsg(GetFile("intermediate-0.csv"))
    nodeProbe1.reply(intermediateFile1)

    receiveOne(5 seconds) match {
      case TaskCompleted(outputFile) =>
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

  test("handle empty partition") {
    val worker = system.actorOf(Props(new ReduceWorker("src/test/resources/data/output")))

    val remoteFiles = Seq(
      RemoteFileAddress(nodeProbe0.ref, "intermediate-0.csv"),
      RemoteFileAddress(nodeProbe1.ref, "intermediate-0.csv")
    )
    val execTask = ExecuteTask(reduceFunc, remoteFiles, 0)
    worker ! execTask

    nodeProbe0.expectMsg(GetFile("intermediate-0.csv"))
    nodeProbe0.reply(Seq.empty[String])
    nodeProbe1.expectMsg(GetFile("intermediate-0.csv"))
    nodeProbe1.reply(Seq.empty[String])

    receiveOne(5 seconds) match {
      case TaskCompleted(outputFile) =>
        outputFile shouldEqual "output/part-001.csv"
    }

    readFile("output/part-001.csv").isEmpty shouldEqual true
    new File("output/part-001.csv").delete()
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

  val reduceFunc: ReduceFunc = {
    case (key, values) =>
      Dataset(
        "country" -> key,
        "userCount" -> values.count.toString
      )
  }
}
