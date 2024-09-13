package Exercise

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DF_sort {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local[*]").appName("Sort").getOrCreate()

    val peopleDF = spark.read.format("csv")
      .option("sep", ",")
      .option("inferschema", "true")
      .option("header", "true")
      .load("data/fakefriends.csv")

    peopleDF.select("*").show()

    //ascending is the default ordering
    //Sort using sort() function
    peopleDF.sort("age","name").show()

    peopleDF.sort(col("age").desc,col("name").desc).show()

    //Using OrderBy()
    peopleDF.orderBy("age","name").show()
    peopleDF.orderBy(col("age").desc,col("name").desc).show()
    peopleDF.orderBy(desc("age"),desc("name")).show()



  }

}
