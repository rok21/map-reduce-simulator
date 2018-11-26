package execution.tasks

import datastructures.JobSpec.{KeyVal, MapFunc}
import datastructures.{Dataset, Row}
import execution.workers.storage.MapWorkerStorage
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class MapTaskTest extends WordSpec with Matchers {

  val mapFunc: MapFunc =
    users => users.map { user =>
      KeyVal(
        key = user("country"),
        value = user
      )
    }

  "read input file, execute map function, partition results and forward to local storage" in {
    implicit val storage = new MapWorkerStorage()
    val task = new MapTask(
      "src/test/resources/data/users/part-001.csv",
      mapFunc = mapFunc,
      numberOfOutputPartitions = 2
    )

    val outputFiles = Await.result(task.execute(storage), 3 seconds)
    outputFiles.size shouldEqual 2

    val file0 = storage.read(outputFiles.head)
    val file1 = storage.read(outputFiles.tail.head)

    Dataset.fromCsv(file0).data.map(row => row(Row.intermediateKeyColumnName)) foreach {
      key => key shouldEqual "DE"
    }

    Dataset.fromCsv(file1).data.map(row => row(Row.intermediateKeyColumnName)) foreach {
      key => key shouldEqual "LT"
    }
  }

}

