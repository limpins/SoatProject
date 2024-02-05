package common.spark.reader

import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.{DataFrame, SparkSession}

trait DataFrameReader {
  def readDataFrame(sparkSession: SparkSession): DataFrame

  def readDataFrameWithFileName(sparkSession: SparkSession): DataFrame = {
    readDataFrame(sparkSession)
      .withColumn("fileName", input_file_name)
  }

}
