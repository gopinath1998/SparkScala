package Exercise

import org.apache.spark.sql._
import org.apache.log4j._

object DF_savemode {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark=SparkSession.builder()
      .master("local[*]")
      .appName("SaveMode")
      .getOrCreate()

    val jsonDF=spark.read.format("JSON").load("data/people.json")
    jsonDF.printSchema()
    jsonDF.show()

    //When data already exists DF is appended to it.
    jsonDF.write.mode("append").format("json").save("output/nameAndFavCol_append.json")

    //When data already exists DF is overwrite to it.
    jsonDF.write.mode("overwrite").format("json").save("output/nameAndFavCol_overwrite.json")

    //When data already exists save operation ignores from saving DF
    jsonDF.write.mode("ignore").format("json").save("output/nameAndFavCol_ignore.json")

    //When data already exists in the destination error thrown
    //jsonDF.write.mode("error").format("json").save("output/nameAndFavCol_error.json")


  }

}
