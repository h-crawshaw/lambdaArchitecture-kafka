package batch

import org.apache.spark.sql.SparkSession

object BatchJob {
  def main (args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Lambda with Spark")
      .master("local[*]")
      .getOrCreate()

    val inputDF = spark
      .read
      //.option("delimiter", "\t")
      .csv("C://Users/harry.crawshaw/Desktop/kafkaconnect/data.tsv")
    inputDF.foreach(println)

    implicit val sqlContext = spark.sqlContext
  }

}
