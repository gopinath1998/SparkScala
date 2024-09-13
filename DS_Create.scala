package Exercise

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}


object DS_Create {
  case class Person(name:String,age:Long)

  def main(args: Array[String]):Unit={

    Logger.getLogger("org").setLevel(Level.ERROR)



    val spark= SparkSession
              .builder()
              .master("local[*]")
              .appName("CreateDS")
              .getOrCreate()

    import spark.implicits._
    //DF without Schema
    val peopleDF=spark.read
                .option("sep",",")
                .csv("data/people.txt")

    peopleDF.printSchema()
    peopleDF.show()

    val nameSchema = new StructType()
      .add("name", StringType, nullable = true)
      .add("age", IntegerType, nullable = true)

    //DF with schema
    val peopleSchemaDF=spark.read
      .option("sep",",")
      .schema(nameSchema)
      .csv("data/people.txt")


    peopleSchemaDF.printSchema()
    peopleSchemaDF.show()

    //DS creation
    val peopleDS=spark.read
      .option("sep",",")
      .schema(nameSchema)
      .csv("data/people.txt")
      .as[Person]

    peopleDS.printSchema()
    peopleDS.show()

  }

}
