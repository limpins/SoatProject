package Service

import model.Data
import org.apache.spark.sql.{Row, SparkSession}
import reader.DataSetReader
import session.SparkProcessor
import writer.DataSetWriter

class DataProcessor(dataReader: DataSetReader[Row],
                    dataWriter: DataSetWriter[Data]) extends SparkProcessor{

  def process(sparkSession: SparkSession): Unit = {
    implicit val session: SparkSession = sparkSession
    import sparkSession.implicits._

    val data = DataService.
      map(dataReader.readDataFrame(sparkSession))
      .as[Data]

    data.show()

    dataWriter.write(data)
  }

}
