package batch

import domain._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object BatchJob {
  def main (args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Lambda with Spark")
      .master("local[*]")
      .getOrCreate()


//    val sourceFile = "C://Users/harry.crawshaw/Desktop/kafkaconnect/data.tsv"
//
//    val input = spark.sparkContext.textFile(sourceFile)
//    input.foreach(println)
//
//    val inputRDD = input.flatMap { line =>
//      val record = line.split("\\t")
//      val MS_IN_HOUR = 1000 * 60 * 60
//      if (record.length == 7)
//      Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR,
//        record(1),
//        record(2),
//        record(3),
//        record(4),
//        record(5),
//        record(6)
//      ))
//      else
//        None
//    }
//    inputRDD.foreach(println)









    val inputSchema = StructType(Array(
      StructField("timestamp", LongType),
      StructField("referrer", StringType),
      StructField("action", StringType),
      StructField("prevPage", StringType),
      StructField("visitor", StringType),
      StructField("page", StringType),
      StructField("product", StringType)
    ))

    import spark.implicits._
    val MS_IN_HOUR: Long = 1000 * 60 * 60
    val inputDF = spark
      .read
      .option("delimiter", "\\t")
      .schema(inputSchema)
      .csv("C://Users/harry.crawshaw/Desktop/kafkaconnect/data.tsv")
      .withColumn("timestamp_hour", ($"timestamp"/3600).cast(LongType) * 100000)
    //    inputDF.foreach(println)
    inputDF.show()
    inputDF.printSchema()

//    val DF = inputDF.select(
//      add_months($"timestamp_hour", 1),
//      $"*"
//    )
//    DF.show()


    //implicit val sqlContext = spark.sqlContext
    import org.apache.spark.sql.functions._


//    val DF = inputDF.select(
//      add_months($"timestamp_hour", 1).as("timestamp_hour")
//    )
//    DF.show()

  }

}
