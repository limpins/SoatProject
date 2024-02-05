package reader.csv

import org.apache.spark.sql.types.StructType

case class CsvReaderParameter(inputFolder: String, options: Map[String, String], schema: Option[StructType] = None)
