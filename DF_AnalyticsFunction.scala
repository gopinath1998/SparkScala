package Exercise

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object DF_AnalyticsFunction {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)


    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("RankFunctions")
      .getOrCreate()

    import spark.implicits._

    val df = Seq(
      (1001, "Satılmış", "İdari", 4000),
      (1002, "Özge", "Personel", 3000),
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
    ).toDF("id", "name", "dept", "salary")


    val windowSpec=Window.partitionBy("dept").orderBy("salary")

    df.withColumn("LEAD",lead("salary",1,0).over(windowSpec))
      .withColumn("LAG",lag("salary",1,0).over(windowSpec))
      .withColumn("CUME_DIST",cume_dist().over(windowSpec))
      //first() is the function equivalent to first_value() function
      .withColumn("First_Value",first("salary").over(windowSpec))
      //last() is the function equivalent to last_value() function
      .withColumn("last_Value",last("salary").over(windowSpec))
      .show()

  }

  }
