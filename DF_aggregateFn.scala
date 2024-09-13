package Exercise

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j._

object DF_aggregateFn {

    def main(args:Array[String]): Unit = {
      Logger.getLogger("org").setLevel(Level.ERROR)

      val spark=SparkSession.builder().master("local[*]").appName("AggregateFunction").getOrCreate()

      val peopleCsvDF= spark.read.format("csv")
        .option("sep",",")
        .option("inferschema","true")
        .option("header","true")
        .load("data/fakefriends.csv")

      peopleCsvDF.select("*").show()

      println("Aggregate function:")
      peopleCsvDF.select(avg("friends") as "avg").show()
      peopleCsvDF.select(min("friends") as "min").show()
      peopleCsvDF.select(max("friends") as "max").show()
      peopleCsvDF.select(sum("friends") as "sum").show()

      peopleCsvDF.select(avg("friends"),min("friends"),max("friends"),sum("friends")).show()

      println("Aggregate using group by:")
      peopleCsvDF.select("*").groupBy("name").count().show()
      peopleCsvDF.select("*").groupBy("name").sum().show()
      peopleCsvDF.select("*").groupBy("name").min().show()
      peopleCsvDF.select("*").groupBy("name").max().show()
      peopleCsvDF.select("*").groupBy("name").agg(round(avg("friends"),2)).alias("avg_friends").show()




  }
}
