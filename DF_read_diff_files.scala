package Exercise

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DF_read_diff_files {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark=SparkSession.builder().master("local[*]").getOrCreate()

    //File read
    //parquet is a default file format for spark hence no need to mention file format
    val userDF=spark.read.load("data/users.parquet")

    userDF.printSchema()
    println("Print all the columns of a DF :")
    userDF.select("*").show()

    //Mention output file format using format() method in write
    //userDF.select("name","favorite_color").write.format("parquet").save("output/nameAndFavCol.parquet")
    println("Reading from CSV")
    val peopleCsvDF= spark.read.format("csv")
                    .option("sep",",")
                    .option("inferschema","true")
                    .option("header","true")
                    .load("data/fakefriends.csv")
    peopleCsvDF.show()


    //Run SQL file directly
    println("Run SQL on files directly")
    val sqlDF=spark.sql("select * from parquet.`data/users.parquet`")
    sqlDF.show()

    println("Read from JSON file :")
    val jsonDF=spark.read.format("JSON").load("data/people.json")
    jsonDF.printSchema()
    jsonDF.show()

  }

}
