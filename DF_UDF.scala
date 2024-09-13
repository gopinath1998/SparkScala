package Exercise

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j._


object DF_UDF {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark=SparkSession.builder().master("local[*]").appName("DF_UDF").getOrCreate()

    // Define and register a zero-argument non-deterministic UDF
    // UDF is deterministic by default, i.e. produces the same result for the same input.
    val random= udf(()=>math.random())
    spark.udf.register("random",random.asNondeterministic())

    //Accessing th UDF that we defined above
    spark.sql("select random()").show()

    //Udf can be directly used with udf.register
    val newRandom=spark.udf.register("newRandom",()=>math.random())
    spark.sql("select newRandom()").show()

    println("Adding one to the input value:")
    val addOne=spark.udf.register("addOne", (x:Int)=> x+1)
    spark.sql("select addOne(6) as new_val").show()

    println("Using udf in WHERE condition")
    spark.udf.register("greaterThan",(x:Int)=> x>5)
    spark.range(1,10).createOrReplaceTempView("rangeTable")
    spark.sql("select * from rangeTable where greaterThan(id)").show()


  }

}
