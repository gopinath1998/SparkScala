package Exercise
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DF_sparkSQL {

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

    //Creating temp view on dataframe
    //Temp view is a session scoped once session is closed view will disappear
    df.createOrReplaceTempView("people_vw")

    //Using .sql function to query on the temp view using sql and return result as DF
    val sqlDf=spark.sql("select * from people_vw")

    sqlDf.show()

    //If we want have a temporary view across the session and alive until spark application terminates it
    //We can use global temporary view
    df.createGlobalTempView("people_glb_vw")

    //global temp view is tied to 'global_temp' database
    spark.sql("select * from global_temp.people_glb_vw").show()

    //global view is cross session
    val new_session=spark.newSession()
    new_session.sql("select * from global_temp.people_glb_vw").show()

    //describing view
    new_session.sql("desc  global_temp.people_glb_vw").show()

  }

}
