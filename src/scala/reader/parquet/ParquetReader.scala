package reader.parquet

class ParquetReader[T](inputFolder: String) extends DataSetReader[T] {

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def readDataFrame(sparkSession: SparkSession): DataFrame = {
    logger.info(s"Loading parquet from path $inputFolder")
    sparkSession.read.parquet(inputFolder)
  }
}
