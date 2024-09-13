package Exercise


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object RDD_print {

  def friends_age(line:String):(Int,Int)={
    val age=line.split(",")(2).toInt
    val friends=line.split(",")(3).toInt

    (age,friends)
  }
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "RDDPrint")

    val line = sc.textFile("data/fakefriends-noheader.csv")

    val friends = line.map(friends_age)

    val numberOfFriends = friends.reduceByKey((x, y) => (x + y))
    println("Sorting the RDD before collect() will not have rigid result:")
    println("Sorting the RDD after collect() will have rigid result:")


    println("Sort the RDD using sortBy() function")
    println("Number of friends by Age:")
    //Before collect() action we can use the ascending in the sortBy() function like below
    val sortedRdd = numberOfFriends.sortBy(_._2, ascending = false)
    sortedRdd.foreach(println)//this sort is not rgid


    println("========================================")
    val sortedRes = numberOfFriends.collect()
    //We cannot use ascending in the sortBy() function below
    // after the collect() function as this goes into scala environment from spark env
    sortedRes.sortBy(_._2).foreach(println)
    //Always use sortBy() with RDD first then print the result instead of sorting it during print


    //Flipping the key and values to sort the RDD by key using sortByKey()
    val flipped = numberOfFriends.map(x => (x._2, x._1))
    flipped.foreach(println)

    println("========================================")
    println("Sort the RDD using sortByKey() function")
    println("Number of friends by Age:")
    val friendsSorted = flipped.sortByKey().collect()
    friendsSorted.foreach(println)

    println("========================================")
    println("Age by AVG number of friends")
    val friendsMap = friends.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    val avgFriends = friendsMap.mapValues(x => x._1 / x._2).collect()
    avgFriends.sortBy(_._1).foreach(println)

    println("========================================")
    println("take() function used to limit the elements of RDD")
    println("========================================")
    avgFriends.sortBy(_._2).take(10).foreach(println)

    println("========================================")
    println("we can also map with println to print the elements of a RDD ")
    println("========================================")
    avgFriends.sortBy(_._2).take(10).map(println)


  }

}
