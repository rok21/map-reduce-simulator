import scala.collection.immutable.{Map => HashMap}
import datastructures.JobSpec.{KeyVal, Map, MapReduce, Reduce}
import execution.SimulationContext

import scala.concurrent.duration._

object Task2 extends App {

  /*

There are two datasets:

- `data/users` dataset with columns "id" and "country"
- `data/clicks` dataset with columns "date", "user_id" and "click_target"

We'd like to produce a new dataset called `data/filtered_clicks`
that includes only those clicks that belong to users from Lithuania (`country=LT`).

 */

  val jobSpec = MapReduce(
    map = Seq(
      Map(
        "data/users",
        users =>
          users
              .filter { user => user("country") == "LT" }
              .map { user =>
                KeyVal(
                  key = user("id"),
                  value = user.updated("table", "users")
                )
              }
      ),
      Map(
       "data/clicks",
       clicks =>
         clicks.map { click =>
           KeyVal(
             key = click("user_id"),
             value = click.updated("table", "clicks")
           )
         }
      )
    ),
    reduce = Reduce {
        case (key, values) =>
          val user = values.filter { value => value("table") == "users" }.headOption
          values.filter { value => value("table") == "clicks" }
                .map { click => click ++ user.getOrElse(HashMap.empty) }
                .filter { click => click.contains("country")  }
    },
    "output/filtered_clicks"
  )

  val sc = new SimulationContext()
  sc.executeJob(
    jobSpec = jobSpec,
    M = 4,
    R = 1
  )
}
