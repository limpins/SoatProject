package stepdefs

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TestContextProvider {

  private var sparkSession: SparkSession = _

  def createSparkSession(): Unit = {
    closeSparkSession()
    sparkSession = SparkSession
      .builder()
      .appName("SoatProject")
      .master("local[2]")
      .config(createSparkConf())
      .getOrCreate()
  }

  private def createSparkConf(): SparkConf = {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.sql.shuffle.partitions", "1")
    sparkConf.set("spark.default.parallelism", "2")
    sparkConf.set("spark.broadcast.compress", "false")
    sparkConf.set("spark.shuffle.compress", "false")
    sparkConf.set("spark.shuffle.spill.compress", "false")
    sparkConf.set("spark.executor.processTreeMetrics.enabled", "false")

    sparkConf
  }

  def closeSparkSession(): Unit = {
    if (Option(sparkSession).isDefined) {
      sparkSession.close()
    }
  }

  def getSparkSession: SparkSession = sparkSession
}
