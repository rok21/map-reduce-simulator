package execution.tasks

import datastructures.{Dataset, Row}
import datastructures.JobSpec.{KeyVal, Map, MapFunc, MapReduce}
import execution.workers.Storage
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class MapTaskTest extends WordSpec with Matchers with MockFactory {

  val mapFunc: MapFunc =
    users => users.map { user =>
      KeyVal(
        key = user("country"),
        value = user
      )
    }

  "map task should read input file, execute map function, partition results and forward to local storage" in {
    val storage = new Storage()
    val task = new MapTask(
      "src/test/resources/data/users/part-001.csv",
      mapFunc = mapFunc,
      numberOfOutputPartitions = 2,
      outputStorage = storage
    )

    val outputFiles = Await.result(task.execute(), 3 seconds)
    outputFiles.foreach(println)
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

