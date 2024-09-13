package Exercise

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel._

object DF_Cache {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local[*]").appName("Cache").getOrCreate()

    val peopleDF = spark.read.format("csv")
      .option("sep", ",")
      .option("inferschema", "true")
      .option("header", "true")
      .load("data/fakefriends.csv")

    val cachedDF=peopleDF.filter(col("age")<50).cache()
    //show(false) does not truncate result where as by default value is truncated
    cachedDF.show(false)

    //MEMORY_AND_DISK level is used by default for DF
    val persistDF=peopleDF.filter(col("age")<50).sort("age").persist()
    //unpersist()
    persistDF.unpersist()

    val memeoryDF=peopleDF.filter(col("age")>50).persist(MEMORY_ONLY)
    val memdisk2DF=peopleDF.filter(col("age")>30).persist(MEMORY_AND_DISK_2)



  }

}
