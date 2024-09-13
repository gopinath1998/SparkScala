package Exercise

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,lit}


object DF_Withcolumn {
  def main(args:Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local[*]").appName("withColumn").getOrCreate()

    val peopleDF = spark.read.format("csv")
      .option("sep", ",")
      .option("inferschema", "true")
      .option("header", "true")
      .load("data/fakefriends.csv")

    peopleDF.select("*").show()

    println("Adding a new constant column")
    //string constant value
    peopleDF.withColumn("new_column",lit("test_val")).show()
    //Integer constant value
    peopleDF.withColumn("new_column",lit(123)).show()

    println("Adding a new column by populating existing column:")
    peopleDF.withColumn("new_age",col("age")+10).show()

    println("Adding multiple column:")
    peopleDF.withColumn("const_string",lit("test_val"))
            .withColumn("const_int",lit(1234))
            .withColumn("new_age",col("age")+10).show()

    println("Change column data type :")
    peopleDF.withColumn("age",col("age").cast("Integer")).show()

    println("Rename existing column name :")
    peopleDF.withColumnRenamed("age","friend's_age").show()

    println("Dropping a column from DF")
    peopleDF.drop("friends").show()


  }

}