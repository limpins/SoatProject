package session

import org.apache.spark.sql.SparkSession

trait SparkProcessor  {
  def process(sparkSession: SparkSession)
}
