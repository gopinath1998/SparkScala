package Exercise

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object DF_repartition_Coalesce {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val peopleSchema=new StructType()
      .add("name",StringType, nullable=false)
      .add("age",IntegerType,nullable = true)

    val spark=SparkSession.builder().master("local[*]").appName("Repartition_Coalesce").getOrCreate()

    val peopleDF=spark.read.format("csv")
      .option("sep",",")
      .schema(peopleSchema)
      .load("data/people.txt")

    //repartition() and coalesce() change the memory partition for a DataFrame


    peopleDF.show()
    println("Convert DF to RDD to the get partition size")
    println(peopleDF.rdd.getNumPartitions)

    println("*****************************************************************")
    println("repartition() is used to increase or decrease the partition size")
    println("*****************************************************************")
    //Default repartition size is 200
    println("Default repartition size is :"+ peopleDF.repartition().rdd.getNumPartitions)
    peopleDF.repartition().show()

   //Repartition to 10 number of partitions
    val partitionedD=peopleDF.repartition(10)
    println("New Partition Size is: "+ partitionedD.rdd.getNumPartitions)
    partitionedD.show()

    //partition based on name column
    println("partition based on name column :")
    val nameDF=peopleDF.repartition(col("name"))
    println("Partition based on name column and default size is  :"+nameDF.rdd.getNumPartitions)

    //partition based on name column with partition size
    println("partition based on name column and specify number of partition :")
    val namenewDF=peopleDF.repartition(7,col("name"))
    namenewDF.show()
    println("Partition based on name column  with partition size of  :"+namenewDF.rdd.getNumPartitions)


    println("*****************************************************************")
    println("coalesce() is used to create new partitions instead of shuffling the data across partitions:")
    println("*****************************************************************")

    val coalesceDF=peopleDF.coalesce(10)
    println("Partition size after coalesce() is : "+coalesceDF.rdd.getNumPartitions)
    //The above returns the partition size as 1 because peopleDF partition size 1 and
    // we cannot increase the partition size by coalesce we can only reduce

    println("partition size after coalesce() is : "+namenewDF.coalesce(5).rdd.getNumPartitions)
    //This return the partition size as 5 because namenewDF has 7 partition and we are reducing the size
  }

}
