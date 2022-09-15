package batch
import org.apache.spark.sql.functions.from_unixtime
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SQLContext, SparkSession}

object BatchJob {
  def main (args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Lambda with Spark")
      .master("local[*]")
      .getOrCreate()


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
    val inputDF = spark
      .read
      .option("delimiter", "\\t")
      .schema(inputSchema)
      .csv("C://Users/harry.crawshaw/Desktop/kafkaconnect/data.tsv") // FROM s3
      .withColumn("timestamp_hour", from_unixtime($"timestamp"/1000))


    implicit val sqlContext: SQLContext = spark.sqlContext
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

//    sqlContext.udf.register("UnderExposed",
//      (pageViewCount: Long, purchaseCount: Long) =>
//      if (purchaseCount == 0) 0 else pageViewCount/purchaseCount
//    )

    val DF = inputDF.select(
      add_months(col("timestamp_hour").cast(TimestampType), 1).as("timestamp_hour"),
      col("timestamp"),
      col("referrer"),
      col("action"),
      col("prevPage"),
      col("visitor"),
      col("page"),
      col("product"))
      .cache()

    DF.createOrReplaceTempView("DF")
    val visitorsByProduct = sqlContext.sql(
      """
        |SELECT product, timestamp_hour, COUNT(DISTINCT visitor) AS unique_visitors
        |FROM DF
        |GROUP BY product, timestamp_hour
        |""".stripMargin)

    val activityByProduct = sqlContext.sql(
      """
        |SELECT
        |product,
        |timestamp_hour,
        |SUM(CASE WHEN action = 'purchase' THEN 1 ELSE 0 END) AS purchase_count,
        |SUM(CASE WHEN action = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_count,
        |SUM(CASE WHEN action = 'page_view' THEN 1 ELSE 0 END) AS page_view_count
        |FROM DF
        |GROUP BY product, timestamp_hour
        |""".stripMargin).cache()

    activityByProduct.show()
//    activityByProduct.write
//      .partitionBy("timestamp_hour")
//      .mode(SaveMode.Append)
//      .parquet("..../batch1") // TO HDFS "hdfs://lambda-app/batch1" maybe
//
//    activityByProduct.createOrReplaceTempView("activityByProduct")

//    val underExposedProducts = sqlContext.sql(
//      """
//        |SELECT
//        |product,
//        |timestamp_hour,
//        |UnderExposed(page_view_count, purchase_count) AS negative_exposure
//        |FROM activityByProduct
//        |ORDER BY negative_exposure DESC
//        |LIMIT 5
//        |""".stripMargin)

    






  }

}
