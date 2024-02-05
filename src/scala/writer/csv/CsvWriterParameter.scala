package writer.csv

import org.apache.spark.sql.SaveMode

case class CsvWriterParameter(targetFolder: Option[String], options: Map[String, String], saveMode: SaveMode = SaveMode.Append)
