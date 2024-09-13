package Exercise

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object RDD_shared_variables {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc= new SparkContext("local[*]","Shared Variable")

    val data=Array(1,2,3,4,5)
    println("Broadcast variables are read only variable cached on each machines in the cluster")

    println("Broadcasting a variable:")
    //Can also be val broadcastvar=sc.broadcast(Array(1,2,3,4,5))
    val broadcastvar=sc.broadcast(data)

    //broadcasting result of rdd
    val csv_file=sc.textFile("data/1800.csv")
    //We need to collect result from RDD to broadcast it
    val items = csv_file.map(x=> x.split(",")).collect()
    val newBroadCast=sc.broadcast(items)

    //After a broadcasting variable is created it should be created instead of data or variable
    //So that data or variable is not shipped to executor more than once
    //Already shipped by broadcasting

    println("Broadcasting variable is accessed using value method:")
    val res=broadcastvar.value
    res.foreach(println)

    println("To release the resources that broadcast variable copied use unpersist() :")
    broadcastvar.unpersist()
    //If broadcast variable is used again it will be re broadcasted

    println("To permanently release the resources use destroy() :")
    broadcastvar.destroy()

    println("****************************************************************")
    println("Accumulators are used for write purpose generally act as a counter or sum across the executors :")
    println("****************************************************************")

    //As a user we can create named and unnamed accumulator
    println("Numeric accumulator can be created by longAccumulator() or doubleAccumulator")

    val accum=sc.longAccumulator("Accumulator")
    //Task running on a executor can add to the accumulator using add() method default value is 0

    sc.parallelize(Array(1,2,3,4,5)).foreach(x=>accum.add(x))
    //The above add the all the elements in the array so it gives sum of all the elements
    println("*** Executors cannot read its value driver program can only read its value***")

    println("Accumulator values can be fetched using value :")
    val accumres=accum.value
    println(accumres)





  }

}
