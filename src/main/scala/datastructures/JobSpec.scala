package datastructures
import scala.collection.immutable.{Map => HashMap}

object JobSpec {

  case class MapReduce(
    map: Seq[Map],
    reduce: Reduce,
    outputDir: String
  )

  type Row = HashMap[String, String]
  type Dataset = Seq[Row]

  case class KeyVal(key: String, value: Row)

  type MapFunc = Dataset => Seq[KeyVal]

  type DataForKey = (String, Dataset)

  type ReduceFunc = DataForKey => Dataset

  case class Map(inputDir: String, mapFunc: MapFunc)


  case class Reduce(reduceFunc: ReduceFunc)
}
