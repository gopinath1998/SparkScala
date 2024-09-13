package Exercise

import com.sundogsoftware.spark.MostObscureSuperheroDataset.SuperHeroNames
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}


object DF_to_DS {

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
      .as[SuperHeroNames]//Converting DF to DS using explicit type casting using case class

    println("Printing Dataset :")
    names.show()

  }
}
