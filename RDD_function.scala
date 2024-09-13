package Exercise

import org.apache.log4j._
import org.apache.spark._
object RDD_function {

  def friends_age(line:String):(Int,Int)={
    val age=line.split(",")(2).toInt
    val friends=line.split(",")(3).toInt

    (age,friends)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc= new SparkContext("local[*]","RDDUsingFunction")

    val line = sc.textFile("data/fakefriends-noheader.csv")

    val friends=line.map(friends_age)

    val numberOfFriends=friends.reduceByKey((x,y)=> (x+y))

    println("Sort the RDD using sortBy() function")
    println("Number of friends by Age:")
    //Before collect() action we can use the ascending in the sortBy() funciton like below
    val sortedRdd=numberOfFriends.sortBy(_._2,ascending=false)
    sortedRdd.foreach(println)


    val sortedRes=numberOfFriends.collect()
    //We cannot use ascending in the sortby() function
    // after the collect() function as this goes into scala environment from spark env
    numberOfFriends.sortBy(_._2).foreach(println)//This sort is not as expected
    //Always use sortBy() with RDD first then print the result instead of sorting it during print



  }

}
