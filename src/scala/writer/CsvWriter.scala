package writer

import org.apache.spark.sql.{Dataset, SaveMode}

class CsvWriter[T](csvWriterParameter: CsvWriterParameter)
  extends DataSetWriter[T] {

  override def write(dataSet: Dataset[T]): Unit = {
    csvWriterParameter.targetFolder match {
      case Some(targetFolderValue) => write(dataSet, targetFolderValue)
      case _                       => showNFirstRows(dataSet, 100)
    }
  }

  private def write(dataSet: Dataset[T], targetFolderValue: String): Unit = {

    dataSet.write
      .mode(csvWriterParameter.saveMode)
      .options(csvWriterParameter.options)
      .csv(targetFolderValue)
  }

  private def showNFirstRows(dataSet: Dataset[T], numberOfRows: Int): Unit = {
    dataSet.show(numberOfRows, truncate = false)
  }

}
