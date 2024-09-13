package Exercise

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.log4j._

object DF_RankFunctions {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)


    val spark=SparkSession.builder()
      .master("local[*]")
      .appName("RankFunctions")
      .getOrCreate()

    import spark.implicits._

    val df = Seq(
      (1001,"Satılmış", "İdari", 4000),
    (1002,"Özge", "Personel", 3000),
    (1003, "Hüsnü", "Bilgi Sistemleri", 4000),
    (1004, "Menşure", "Muhasebe", 6500),
    (1005, "Doruk", "Personel", 3000),
    (1006, "Şilan", "Muhasebe", 5000),
    (1007, "Baran", "Personel", 7000),
    (1008, "Ülkü", "İdari", 4000),
    (1009, "Cüneyt", "Bilgi Sistemleri", 6500),
    (1010, "Gülşen", "Bilgi Sistemleri", 7000),
    (1011, "Melih", "Bilgi Sistemleri", 8000),
    (1012, "Gülbahar", "Bilgi Sistemleri", 10000),
    (1013, "Tuna", "İdari", 2000),
    (1014, "Raşel", "Personel", 3000),
    (1015, "Şahabettin", "Bilgi Sistemleri", 4500),
    (1016, "Elmas", "Muhasebe", 6500),
    (1017, "Ahmet Hamdi", "Personel", 3500),
    (1018, "Leyla", "Muhasebe", 5500),
    (1019, "Cuma", "Personel", 8000),
    (1020, "Yelda", "İdari", 5000),
    (1021, "Rojda", "Bilgi Sistemleri", 6000),
    (1022, "İbrahim", "Bilgi Sistemleri", 8000),
    (1023, "Davut", "Bilgi Sistemleri", 8000),
    (1024, "Arzu", "Bilgi Sistemleri", 11000)
    ).toDF("id", "name", "dept","salary")

    df.show()

    df.createOrReplaceTempView("employee")

    spark.sql("select * from employee").show()

    //Specify window directly in the over clause
    df.withColumn("Row Number",row_number()over(Window.partitionBy("dept").orderBy("salary"))).show()

    //Specify windows separately and use it in the over clause
    println("Rank function in Dataframe:")
    val windowSpec=Window.partitionBy("dept").orderBy("salary")
    df.withColumn("Row_Number",row_number()over(windowSpec))
      .withColumn("Rank",rank()over(windowSpec))
      .withColumn("Dense_Rank",dense_rank()over(windowSpec))
      .withColumn("percent_rank",percent_rank()over(windowSpec))
      .withColumn("ntile",ntile(2)over(windowSpec))
      .select("name","dept","Row_Number","Rank","Dense_Rank","percent_rank","ntile").show()

    println("Filter the dataset with rank=1")
    df.withColumn("Rank",rank()over(windowSpec)).filter(col("Rank")===1).show()
    //column name is case insensitive  Dense_Rank can be given as dense_Rank
    df.withColumn("Dense_Rank",dense_rank()over(windowSpec)).where(col("dense_Rank")===1).show()
  }


}
