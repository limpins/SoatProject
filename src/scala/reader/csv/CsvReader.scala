package reader.csv

import com.socgen.cprofit.dtl.common.spark.reader.DataSetReader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

class CsvReader[T](csvReaderParameter: CsvReaderParameter) extends DataSetReader[T] {

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def readDataFrame(sparkSession: SparkSession): DataFrame = {
    val inputPath = csvReaderParameter.inputFolder
    var csvReader = sparkSession.read
      .format("csv")
      .options(csvReaderParameter.options)

    csvReaderParameter.schema.foreach(sch => csvReader = csvReader.schema(sch))

    logger.info(s"Loading csv from path $inputPath")
    csvReader
      .load(inputPath)
  }

}