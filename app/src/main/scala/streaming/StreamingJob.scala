package streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object StreamingJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Lambda with Spark")
      .master("local[*]")
      .getOrCreate()

    val batchDuration = "2 seconds" //writestream.trigger(processingTime=...)

    val inputtestSchema = StructType(Array(
      StructField("timestamp", LongType),
      StructField("referrer", StringType),
      StructField("action", StringType),
      StructField("prevPage", StringType),
      StructField("visitor", StringType),
      StructField("page", StringType),
      StructField("product", StringType)
    ))


    val testDF = spark.readStream
      .option("delimiter", "\\t")
      .schema(inputtestSchema)
      .csv("C://Users/harry.crawshaw/Desktop/kafkaconnect/input")

    testDF.writeStream
      .format("parquet")
      .option("path", "C://Users/harry.crawshaw/Desktop/kafkaconnect/output")
      .option("checkpointLocation", "C://Users/harry.crawshaw/Desktop/kafkaconnect/checkpoints")
      .start()
      .awaitTermination()
  }

}
