package Exercise

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object RDD_action {

    def friends_age(line: String): (Int, Int) = {
      val age = line.split(",")(2).toInt
      val friends = line.split(",")(3).toInt

      (age, friends)
    }

    def main(args: Array[String]): Unit = {
      Logger.getLogger("org").setLevel(Level.ERROR)

      val sc = new SparkContext("local[*]", "RDDAction")

      val line = sc.textFile("data/fakefriends-noheader.csv")

      val friends = line.map(friends_age)

      val numberOfFriends = friends.reduceByKey((x, y) => (x + y))

      val sortedRdd = numberOfFriends.sortBy(_._2, ascending = false)
      sortedRdd.foreach(println)

      println("*************************************************************************************")
      println("Below actions does not return any rdd they return the result set to the driver program")
      println("*************************************************************************************")


      println("collect() returns the RDD to the driver program: ")
      val rdd=sortedRdd.collect()
      rdd.foreach(print)

      val countres=sortedRdd.count().toInt

      println("\ntake(n) function returns the first n number of elements of a RDD")
      val takefive=sortedRdd.take(5)
      takefive.foreach(println)
      println("-----------------------------------")
      val takefirst=sortedRdd.take(1)
      takefirst.foreach(println)


      println("\nfirst() used to fetch first row element of the RDD: similar to take(1)")
      val firstres=sortedRdd.first()
      //TO print the value we cannot use the foreach as we have only one elements returned
      println(firstres)




    }
  }