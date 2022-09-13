package clickstream

import config.Settings

object LogProducer extends App {
  // WebLog config
  val wlc = Settings.WebLogGen

  val Products = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/products.csv")).getLines().toArray
  val Referrers = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/referrers.csv")).getLines().toArray
  
}
