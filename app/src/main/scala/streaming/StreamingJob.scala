package streaming

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{SQLContext, SparkSession}

object StreamingJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Lambda with Spark")
      .master("local[*]")
      .getOrCreate()
    implicit val sqlContext: SQLContext = spark.sqlContext

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
      .withWatermark("timestamp", "5 minutes")//

//    testDF.writeStream
//      .format("console")
//     // .trigger("processingTime", "4 seconds")
//    //  .option("path", "C://Users/harry.crawshaw/Desktop/kafkaconnect/output")
//     // .option("checkpointLocation", "C://Users/harry.crawshaw/Desktop/kafkaconnect/checkpoints")
//      .start()
//      .awaitTermination()

//    val testing = spark.read.parquet("C://Users/harry.crawshaw/Desktop/kafkaconnect/output/tee1.snappy.parquet")
//    testing.show()

    testDF.createOrReplaceTempView("testDF")
//
  //  activityByProduct =
    val sqlDF = spark.sql(
      """
        |SELECT
        |product,
        |timestamp,
        |SUM(CASE WHEN action = 'purchase' THEN 1 ELSE 0 END) AS purchase_count,
        |SUM(CASE WHEN action = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_count,
        |SUM(CASE WHEN action = 'page_view' THEN 1 ELSE 0 END) AS page_view_count
        |FROM testDF
        |GROUP BY product, timestamp
        |""".stripMargin)


    sqlDF.writeStream
      .format("console")
      .start()
      .awaitTermination()

  }

}
