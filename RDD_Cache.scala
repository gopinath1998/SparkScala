package Exercise

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

object RDD_Cache {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Cache")

    val csv_file=sc.textFile("data/1800.csv")

    val items = csv_file.map(x=> x.split(","))
    //When we assign rdd to rdd without any transformation the both are considered the same
    // val itemsrdd=items // both rdd are considered the same

    val newrdd=items.map(x=>x)
    //As we are mapping(transformation) newrdd is considered as the new RDD
    //though we are mapping the same line to same line

    //Cache is used when we need to the RDD in multiple places

    println("Cache stores the rdd in the memory only of worker node for RDD")
    items.cache()

    //Persist also do the same job as cache
    //but it has flexibility to store it the various StorageLevels

    println("MEMORY_ONLY persist the result in the memory of worker nodes")
    // it is the default for RDD
    items.persist(StorageLevel.MEMORY_ONLY)

    println("***NOTE we Cannot change storage level of an RDD after it was already assigned a level***")

    print("DISK_ONLY stores the results in the local disc of a worker node:")
    newrdd.persist(StorageLevel.DISK_ONLY)

    //MEMORY_AND_DISK stores the result in the memory and if it full then stores it in the disk
    //items.persist(StorageLevel.MEMORY_AND_DISK)



  }

}
