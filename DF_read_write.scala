package Exercise

import org.apache.spark.sql._
import org.apache.log4j._

object DF_read_write {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark=SparkSession.builder().master("local[*]").getOrCreate()

    //File read
    //parquet is a default file format for spark hence no need to mention file format
    val userDF=spark.read.load("data/users.parquet")

    userDF.printSchema()
    println("Print all the columns of a DF :")
    userDF.select("*").show()

    println("Writing a  DF :")
    userDF.select("name","favorite_color").write.save("output/nameAndFavCol.parquet")





  }

}
