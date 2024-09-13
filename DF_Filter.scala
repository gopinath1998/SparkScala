package Exercise

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object DF_Filter {
  case class SuperHeroNames(id:Int,name:String)

  def main(args:Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local[*]").appName("DFtoDS").getOrCreate()

    // Create schema when reading Marvel-names.txt
    val superHeroSchema= new StructType()
      .add("id",IntegerType,nullable = true)
      .add("name",StringType,nullable=true)

    // Build up a hero ID -> name Dataset
    import spark.implicits._
    val names = spark.read
      .schema(superHeroSchema)
      .option("sep", " ")
      .csv("data/Marvel-names.txt")
      .as[SuperHeroNames]//Converting DF to DS using explicit type sating using case class

    names.show()

    println("Filtering DS using filter function:")
    names.filter(col("id")===1).show()
    names.filter(col("name")==="ABEL").show()
    names.filter(lower(col("name"))==="abel").show()

    println("Filtering DS using WHERE function:")
    names.where(col("id")===20).show()
    names.where(col("name")==="ABBOTT, JACK").show()


    println("Filtering using wild card function:")
    names.filter(col("name").like("%AB%")).show()
    names.filter(col("name").like("AB%")).show()
    names.filter(col("name").like("ABE_")).show()
    names.filter(col("name").like("__SALOM")).show()

    names.filter(lower(col("name")).like("__salom")).show()

  }

}
