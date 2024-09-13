package Exercise

import org.apache.spark.sql._
import org.apache.log4j._

import org.apache.spark.sql.functions.{col, size, split, sum}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object DF_join {

  case class SuperHeroNames(id: Int, name: String)
  case class SuperHero(value: String)
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("DFJoin").master("local[*]").getOrCreate()

    val superHeroNamesSchema = new StructType()
      .add("id", IntegerType, nullable = true)
      .add("name", StringType, nullable = true)

    import spark.implicits._
    val names = spark.read
      .schema(superHeroNamesSchema)
      .option("sep", " ")
      .csv("data/Marvel-names.txt")
      .as[SuperHeroNames]
    names.show()

    val lines = spark.read
      .text("data/Marvel-graph.txt")
      .as[SuperHero]

    val connections = lines
      .withColumn("id", split(col("value"), " ")(0))
      .withColumn("connections", size(split(col("value"), " ")) - 1)
      .groupBy("id").agg(sum("connections").alias("connections"))

    connections.show()
    println("Inner join")
    connections.join(names,"id").show()

    connections.join(names,connections("id")===names("id")).show()
    println("Select the columns using column names:")
    connections.join(names,connections("id")===names("id"))
      .select(connections("id"),connections("connections").alias("connection"),
        names("id"),names("name")).show()


    println("Left join")
    connections.join(names,connections("id")===names("id"),"left").show()

    println("Right join")
    connections.join(names,connections("id")===names("id"),"right").show()

    println("Full outer join")
    connections.join(names,connections("id")===names("id"),"left").show()

  }
}
