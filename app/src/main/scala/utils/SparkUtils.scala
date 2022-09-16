package utils

import java.lang.management.ManagementFactory

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{StreamingContext, Duration}
import org.apache.spark.{SparkContext, SparkConf}

object SparkUtils {
  val isIDE = {
    ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
  }
  def getSparkContext(appName: String) = {
    var checkpointDirectory = ""

    // get spark configuration
    val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.casandra.connection.host", "localhost")

    // Check if running from IDE
    if (isIDE) {
      System.setProperty("hadoop.home.dir", "F:\\Libraries\\WinUtils") // required for winutils
      conf.setMaster("local[*]")
      checkpointDirectory = "file:///f:/temp"
    } else {
      checkpointDirectory = "hdfs://lambda-pluralsight:9000/spark/checkpoint"
    }