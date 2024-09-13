package Exercise
import org.apache.log4j._
import org.apache.spark.SparkContext

object RDD_operations {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc= new SparkContext("local[*]","RDDOperations")

    val lines= sc.textFile("data/book.txt")

    val lineLength=lines.map(x=>x.length)
    lineLength.foreach(println)

    //reduce return to the scala environment so we can print statement
    val totalLength=lineLength.reduce((x,y)=>x+y)
    println(s"Total line length is $totalLength")





  }

}
