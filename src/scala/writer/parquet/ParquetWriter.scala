package writer.parquet

import org.apache.log4j.LogManager
import org.apache.spark.sql.{Dataset, SaveMode}

class ParquetWriter[T](targetFolder: Option[String],
                       saveMode: SaveMode = SaveMode.Overwrite,
                       partitionColumnNames: Option[Array[String]] = None)
  extends DataSetWriter[T] {

  private val logger = LogManager.getLogger(this.getClass)

  override def write(dataSet: Dataset[T]): Unit = {
    targetFolder match {
      case Some(targetFolderValue) => writeParquet(dataSet, targetFolderValue)
      case _                       => showNFirstRows(dataSet, 100)
    }
  }

  private def writeParquet(dataSet: Dataset[T], targetFolderValue: String): Unit = {
    logger.info(s"Writing parquet in folder $targetFolderValue")

    var dataFrameWriter = dataSet.write
      .mode(saveMode)

    dataFrameWriter = setPartitionBy(partitionColumnNames, dataFrameWriter)

    dataFrameWriter.parquet(targetFolderValue)
  }

  private def showNFirstRows(dataSet: Dataset[T], numberOfRows: Int): Unit = {
    dataSet.show(numberOfRows, truncate = false)
  }

}