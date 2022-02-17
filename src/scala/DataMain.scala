import Service.DataProcessor
import config.IOConfig.{configCsvIn, configCsvOut}
import config.{ApplicationConfigReader, ProjectConfig}
import model.Data
import session.{SparkJobLauncher, SparkJobParameter, SparkSessionBuilder}
import org.apache.spark.sql.Row
import reader.{CsvDataFrameReader, CsvReaderParameter, DataSetReader}
import writer.{CsvWriter, CsvWriterParameter}

object DataMain {

  def main(args: Array[String]): Unit = {

    val applicationName = "SoatProject"

    val projectConfig = new ProjectConfig(args)

    val applicationConfig = ApplicationConfigReader.read(projectConfig.applicationConfigPath.toOption.get)
      .getOrElse(throw new IllegalArgumentException("can't parse config"))

    val sparkSession = SparkSessionBuilder.createSparkSession(applicationName)

    def getDataReader(csvConfig: CsvReaderParameter): DataSetReader[Row] = {
      new CsvDataFrameReader[Row](csvConfig)
    }

    def getDataWriter(csvConfig: CsvWriterParameter): CsvWriter[Data] = {
      new CsvWriter[Data](csvConfig)

    }

    val dataProcessor = new DataProcessor(
      getDataReader(configCsvIn),
      getDataWriter(configCsvOut)
    )



    new SparkJobLauncher(
      SparkJobParameter(
        sparkProcessor = dataProcessor,
        sparkSession = sparkSession
      )
    ).launch()

  }

}
