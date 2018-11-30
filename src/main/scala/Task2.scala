import datastructures.{Dataset, Row}
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
              .select { user => user("country") == "LT" }
              .map { user =>
                KeyVal(
                  key = user("id"),
                  value = user.merge("table" -> "users")
                )
              }
      ),
      Map(
       "data/clicks",
       clicks =>
         clicks.map { click =>
           KeyVal(
             key = click("user_id"),
             value = click.merge("table" -> "clicks")
           )
         }
      )
    ),
    reduce = Reduce {
        case (key, values) =>
          val user = values.select { value => value("table") == "users" }.first
          values.select { value => value("table") == "clicks" }
                .mapr { click => click.merge(user.getOrElse(Row.empty)) }
                .select { click => click.contains("country")  }
    },
    "output/filtered_clicks"
  )

  val sc = new SimulationContext()
  sc.executeJob(
    jobSpec = jobSpec,
    M = 4,
    R = 1,
    maxDuration = 5 seconds
  )
}

/*
```ruby
  MapReduce(
    map: {
      'data/users' => lambda { |users|
        users
          .select { |user| user['country'] == 'LT' }
          .map { |user|
            {
              key: user['id'],
              value: user.merge('table' => 'users')
            }
          }
      },
      'data/clicks' => lambda { |clicks|
        clicks.map { |click|
          {
            key: click['user_id'],
            value: click.merge('table' => 'clicks')
          }
        }
      }
    },
    reduce: lambda { |key, values|
      user = values.select { |value| value['table'] == 'users' }.first

      values
        .select { |value| value['table'] == 'clicks' }
        .map { |click| click.merge(user || {}) }
        .select { |click| !click['country'].nil? }
    },
    output: 'data/filtered_clicks'
  )
```
 */
