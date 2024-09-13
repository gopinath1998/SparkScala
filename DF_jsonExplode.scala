package Exercise

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j._

object DF_jsonExplode {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark=SparkSession.builder().appName("JsonExplode").master("local[*]").getOrCreate()

    //data will be shown in a column named _corrupted_record as there are line multi line
    val rawDF=spark.read.json("data/jsonData.json")
    rawDF.show()
    rawDF.printSchema()


    //To avoid the _corrupted_record error we can use wholeTextFiles method
    //Which will give us RDD which can be converted to DF
    val rddDF=spark.read.json(spark.sparkContext.wholeTextFiles("data/jsonData.json").values)
    rddDF.show()
    rddDF.printSchema()

    val multiLineDF=spark.read.option("multiline",true).json("data/jsonData.json")

    multiLineDF.show()
    multiLineDF.printSchema()

    //explode() function takes array values and create one per values in the array
   val tempDF= multiLineDF.select(explode(col("students")).as("students"))
    tempDF.show(false)
    tempDF.printSchema()

    println("Explode with all other columns: ")
    println("printing all the values of DF")
    val allvalDF=multiLineDF.select(col("desc"),col("total_record"),explode(col("students")).as("students"))
    allvalDF.show()

    println("Printing all the values along with all the values of array(struct type) in a separate column ;")
    allvalDF.select(col("desc"),col("total_record"),col("students.*")).show()
    allvalDF.printSchema()
    //Above schema shows that student is of type struct
    //we can use dot(.) to get all the columns

    val finalresDF= tempDF.select(col("students.*"))
    finalresDF.show(false)
    finalresDF.printSchema()

  }

}
