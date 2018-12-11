import datastructures.Dataset
import datastructures.JobSpec.{KeyVal, Map, MapReduce, Reduce}
import execution.SimulationContext
import scala.concurrent.duration._
import scala.collection.immutable.{Map => HashMap}

object Task1 extends App {

  /*

  Having in mind `data/clicks` dataset with "date" column,
  count how many clicks there were for each date
  and write the results to `data/total_clicks` dataset with "date" and "count" columns.

  */

  val jobSpec = MapReduce(
    map = Seq(
      Map(
        "data/clicks",
        clicks => clicks.map { click =>
          KeyVal(
            key = click("date"),
            value = click
          )
        }
      )
    ),
    reduce = Reduce(
      {
        case (key, values) =>
          Seq(HashMap(
            "date" -> key,
            "count" -> values.size.toString
          ))
      }
    ),
    "output/clicks_per_day"
  )

  val sc = new SimulationContext()
  sc.executeJob(
    jobSpec = jobSpec,
    M = 4,
    R = 1
  )
}