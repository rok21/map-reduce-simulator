import datastructures.Dataset
import datastructures.JobSpec.{KeyVal, Map, MapReduce, Reduce}
import execution.SimulationContext
import scala.concurrent.duration._

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
          Dataset(
            "date" -> key,
            "count" -> values.count.toString
          )
      }
    ),
    "output/clicks_per_day"
  )

  val sc = new SimulationContext()
  sc.executeJob(
    jobSpec = jobSpec,
    M = 4,
    R = 1,
    maxDuration = 1 hour
  )
}

/*
## Task #1: use implemented map-reduce framework for aggregation
```ruby
  MapReduce(
    map: {
      'data/clicks' => lambda { |clicks|
        clicks.map { |click|
          {
            key: click['date'],
            value: click
          }
        }
      }
    },
    reduce: lambda { |key, values|
      [
        {
          'date' => key,
          'count' => values.count
        }
      ]
    },
    output: 'data/clicks_per_day'
  )
```
 */
