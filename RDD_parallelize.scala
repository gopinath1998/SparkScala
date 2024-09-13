package Exercise
import org.apache.log4j._
import org.apache.spark.SparkContext

object RDD_parallelize {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc= new SparkContext("local[*]","parallelize")

    val data=Array(1,2,3,4,5)

    //Parallelized collections are created using sparkcontext's parallelize() method
    val distdata=sc.parallelize(data)
    distdata.foreach(println)

    //Spark automatically creates a partition,we can specify the partition like below
    val partitioned_data=sc.parallelize(data,10)

    partitioned_data.foreach(println)




  }

}
