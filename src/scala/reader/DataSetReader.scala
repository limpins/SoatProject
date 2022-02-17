package reader

import org.apache.spark.sql.{DataFrame, SparkSession}

trait DataSetReader[T] {
  def readDataFrame(sparkSession: SparkSession): DataFrame

}
