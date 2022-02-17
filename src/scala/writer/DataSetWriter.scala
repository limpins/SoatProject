package writer

import org.apache.spark.sql.{DataFrameWriter, Dataset}

trait DataSetWriter[T] {
  def write(dataSet: Dataset[T]): Unit

  def setPartitionBy(columnNames: Option[Array[String]], writer: DataFrameWriter[T]): DataFrameWriter[T] = {
    columnNames
      .flatMap(colNames => Some(writer.partitionBy(colNames: _*)))
      .getOrElse(writer)
  }
}
