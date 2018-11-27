package datastructures

object JobSpec {

  case class MapReduce(
    map: Map,
    reduce: Reduce,
    outputDir: String
  )

  case class KeyVal(key: String, value: Row)

  type MapFunc = Dataset => Seq[KeyVal]

  type ReduceFunc = DataForKey => Dataset

  case class Map(inputDir: String, mapFunc: MapFunc)

  case class DataForKey(key: String, values: Dataset)

  case class Reduce(reduceFunc: ReduceFunc)
}
