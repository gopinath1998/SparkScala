package Exercise

import org.apache.spark.sql.SparkSession
import org.apache.log4j._

object DF_create {

  def main(args: Array[String]):Unit={

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
                .builder()
                .appName("SparkSessionCreation")
                //.config("spark.config","some-value")
                .master("local[*]")
                .getOrCreate()

    val df=spark.read.json("data/people.json")

    df.printSchema()
    df.show()


  }

}
