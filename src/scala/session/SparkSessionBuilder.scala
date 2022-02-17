package session

import org.apache.spark.sql.SparkSession

object SparkSessionBuilder {

  def createSparkSession(applicationName: String): SparkSession = {
    getDefaultSparkSessionBuilder(applicationName)
      .getOrCreate()
  }

  private def getDefaultSparkSessionBuilder(applicationName: String): SparkSession.Builder = {

    var sparkSessionBuilder = SparkSession
      .builder()
      .appName(applicationName)

    sparkSessionBuilder = addLocalConfiguration(sparkSessionBuilder)

    sparkSessionBuilder
  }

  private def addLocalConfiguration(sparkSessionBuilder: SparkSession.Builder): SparkSession.Builder = {
    sparkSessionBuilder.master("local[2]")
    sparkSessionBuilder.config("spark.sql.shuffle.partitions", "1")
    sparkSessionBuilder.config("spark.default.parallelism", "2")
    sparkSessionBuilder.config("spark.executor.processTreeMetrics.enabled", "false")
  }

}
