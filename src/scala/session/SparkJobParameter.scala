package session

import org.apache.spark.sql.SparkSession

case class SparkJobParameter(sparkProcessor: SparkProcessor, sparkSession: SparkSession, closeable: Boolean = true)
