package reader

import org.apache.spark.sql.{DataFrame, SparkSession}

class CsvDataFrameReader[T](csvReaderParameter: CsvReaderParameter) extends DataSetReader[T] {

  override def readDataFrame(sparkSession: SparkSession): DataFrame = {
    val inputPath = csvReaderParameter.inputFolder
    var csvReader = sparkSession.read
      .format("csv")
      .options(csvReaderParameter.options)

    csvReaderParameter.schema.foreach(sch => csvReader = csvReader.schema(sch))

    csvReader
      .load(inputPath)
  }
}
