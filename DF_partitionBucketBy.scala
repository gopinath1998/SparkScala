package Exercise

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object DF_partitionBucketBy {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val peopleSchema = new StructType()
      .add("name", StringType, nullable = false)
      .add("age", IntegerType, nullable = true)

    val spark = SparkSession.builder().master("local[*]").appName("PartitionBybucketBy").getOrCreate()

    val peopleDF = spark.read.format("csv")
      .option("sep", ",")
      .schema(peopleSchema)
      .load("data/people.txt")

    //Spark writer allows for data to be partitioned on disk with partitionBy
    //partitionBy() is DataFrameWriter method

    peopleDF.write.mode("overwrite").partitionBy("name").format("csv").save("data/output/partitionBy")
    //Each partition for each unique name with file  as name={unique partition by column values}
    //one file per partition will be created
    //result file will not have the partitionBy column values as the results are inside that column folder


    println("repartion by name and partition by name during disc write:")
    peopleDF.repartition(col("name")).write.mode("overwrite")
      .partitionBy("name").format("csv").save("data/output/partitionByOverWrite")

    println("*******************************************************")
    println("bucketBy function to create buckets of data in the memory :")
    println("*******************************************************")
    //bucketBy(number_of_bucket,column_name_for_bucket)
    // we cannot save bucketBy result as file and need to save as table
    //saveAsTable creates a spark-warehouse folder and inside in create a folder with table name
    peopleDF.write.mode("overwrite").bucketBy(4,"name").format("csv").saveAsTable("bucketByTable")

    peopleDF.write.mode("overwrite").bucketBy(4,"name").sortBy("age").format("csv").saveAsTable("bucketByTableSorted")





  }
}
