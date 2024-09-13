package Exercise

import org.apache.log4j._
import org.apache.spark.sql._

object DF_select {

  def main(args: Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.ERROR)


    val spark = SparkSession
      .builder()
      .appName("SparkSessionCreation")
      .config("spark.config","some-value")
      .master("local[*]")
      .getOrCreate()

    val df=spark.read.json("data/people.json")

    df.printSchema()

    df.select("name").show()

    df.select("name","age").show()

    //implicits implicitly converts scala objects into DF,DS and columns
    //needed for using $ symbol
    import spark.implicits._

    //Add age plus to all
    // df.select("name","age"+1).show()
    //Above "age"+1 will be considered as "age+1" column so error is thrown
    df.select($"name",$"age"+1).show()

    //Filter based on age
    df.filter($"age"<50).show()

    //Count people by age
    df.groupBy("age").count().show()

  }

}
