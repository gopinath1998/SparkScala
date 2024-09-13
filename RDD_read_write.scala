package Exercise

import org.apache.log4j._
import org.apache.spark._

object RDD_read_write {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "ReadWrite")

    val lines = sc.textFile("data/ml-100k/u.data")

    val words = lines.flatMap(x => x.split(","))

    //writes RDD into text file, result may have multiple files due to partition where data stored
    words.saveAsTextFile("Output/text_RDD")

    //Writes the result as a single file, repartition can be used to save RDD in many files
    words.repartition(1).saveAsTextFile("Output/text_RDD_single_file")

    val csv_file=sc.textFile("data/1800.csv")

    val items = csv_file.map(x=> x.split(","))

    //items.repartition(1).saveAsTextFile("Output/csv_RDD_single_file.csv")

    items.repartition(1).saveAsTextFile("Output/csv_RDD_single_file")

    sc.stop()
  }
}


