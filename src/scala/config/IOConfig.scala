package config

import reader.CsvReaderParameter
import writer.CsvWriterParameter

object IOConfig {

  val readCsvOptions = Map(
    "inferSchema" -> "false",
    "header" -> "true",
    "delimiter" -> ";",
    "mode" -> "permissive",
    "escape" -> "\""
  )

  val WriteCsvOptions = Map(
    "header" -> "true",
    "delimiter" -> ";"
  )

  val inputCsv = "C:\\inputCsv\\data.csv"
  val outputCsv = "C:\\outputCsv\\output.csv"

  val configCsvIn = CsvReaderParameter(inputCsv, readCsvOptions)
  val configCsvOut = CsvWriterParameter(Some(outputCsv), WriteCsvOptions)

}
