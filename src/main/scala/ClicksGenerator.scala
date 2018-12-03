import io.DiskIOSupport

import scala.util.Random

object ClicksGenerator extends App with DiskIOSupport {
  def randomClick = {
    val rand = new Random()
    val date = s"2017-12-${rand.nextInt(31)+1}"
    val userId = rand.nextInt(18)
    val target = "idk"
    Seq(date, userId.toString, target).mkString(",")
  }

  def genFile(id: Int): Unit = {
    if(id < 100) {
      val file = for {
        _ <- 0 to 10000
      } yield randomClick
      println(s"creating file $id")
      val headerRow = "date,user_id,target"
      writeFile(s"bigdata/part-$id.csv", headerRow+:file)
      genFile(id + 1)
    }
  }

  genFile(0)
}
