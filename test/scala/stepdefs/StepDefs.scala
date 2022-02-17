package stepdefs

import cucumber.CucumberMapper
import model.Data
import writer.ResultWriter
import cucumber.api.DataTable
import gherkin.pickles.PickleRow
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.Assertion
import org.scalatest.Matchers._
import reader.DataReader
import Service.DataProcessor

class StepDefs extends CommonStepDefs {

  var dataReader: DataReader[Row] = _
  val resultWriter = new ResultWriter[Data]

  Before { _ =>
    resultWriter.clear()
    dataReader = new DataReader[Row](Seq.empty)
  }

  Given("""^the data$""") { dataRow: DataTable =>
    dataReader = new DataReader[Row](
      new CucumberMapper[Data](getRows(dataRow)).createSequence())
  }

  When("""^I process scenario 1$""") { () =>
    new DataProcessor(dataReader,
      resultWriter).process(retrieveSparkSession)
  }

  Then("""^The result should be$""") { dataRow: DataTable =>
    val expectedData = new CucumberMapper[Data](getRows(dataRow)).createSequence()

    val currentData = resultWriter.result

    currentData should contain theSameElementsAs expectedData
  }

  def getRows(datable: DataTable): Iterable[PickleRow] = {
    import scala.collection.JavaConverters._
    datable.getPickleRows.asScala
  }

}
