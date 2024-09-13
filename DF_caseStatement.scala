package Exercise

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}


object DF_caseStatement {

  def main(args:Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().master("local[*]").appName("caseStatement").getOrCreate()

    val peopleSchema=new StructType()
      .add("name",StringType, nullable=false)
      .add("age",IntegerType,nullable = true)

    val peopleDF=spark.read.format("csv")
                .option("sep",",")
                .schema(peopleSchema)
                .load("data/people.txt")

    peopleDF.show()

    val new_DF=peopleDF.withColumn("ageCategory",when(col("age")<20 ,lit("Below twenty"))
                                                 .when(col("age")>=20 and col("age")<=30,lit("Between twenty and Thirty"))
                                                 .otherwise(lit("Above Thirty"))
                        )
    new_DF.show(truncate=false)






  }

}
