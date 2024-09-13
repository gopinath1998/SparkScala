package Exercise

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object DF_sharedVariables {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local[*]").appName("SharedVariable").getOrCreate()

    val friendsDF = spark.read.format("csv")
      .option("sep", ",")
      .option("inferschema", "true")
      .option("header", "true")
      .load("data/fakefriends.csv")

    friendsDF.show()


    val nameSchema = new StructType()
      .add("name", StringType, nullable = true)
      .add("age", IntegerType, nullable = true)

    val peopleDF=spark.read
      .option("sep",",")
      .schema(nameSchema)
      .csv("data/people.txt")

    println("Broadcast variable in DataFrame :")
    //sparkContext is used for broadcasting
    spark.sparkContext.broadcast(peopleDF)

    //Broadcasting can be done during join operation import broadcast function
    val joinedDF=friendsDF.join(broadcast(peopleDF))
    //The EXPLAIN statement is used to provide logical/physical plans for an input statement.
    // By default, this clause provides information about a physical plan only.
    joinedDF.explain()
    //extended=true means it returns all the plans
    joinedDF.explain(extended = true)

    println("Accumulator in DataFrame :")

  //Accumulators can be created using the spark context object as shown below
    //val sc=spark.sparkContext
   //
  val accum=spark.sparkContext.longAccumulator("countAccumulator")
    //add 1 to accumulator for every records
    joinedDF.foreach(x=>accum.add(1))
    println("Number of records:"+ accum.value.toInt)

    val sumaccum=spark.sparkContext.longAccumulator("sumAccumulator")
    //add 1 to accumulator for every records
    joinedDF.foreach(x=>sumaccum.add(x(2).toString.toInt))
    println("Sum of friends Age:"+ sumaccum.value.toInt)


    //count number of friends whose age is less than 30
    val minorAccum=spark.sparkContext.longAccumulator("minorAccumulator")
    joinedDF.foreach(x=>if(x(2).toString.toInt<30) minorAccum.add(1))
    println("Number of Minors in the dataset:"+ minorAccum.value.toInt)



  }

  }
