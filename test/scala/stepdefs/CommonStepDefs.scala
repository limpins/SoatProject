package stepdefs

import cucumber.api.scala.{EN, ScalaDsl}
import org.apache.spark.sql.SparkSession

class CommonStepDefs extends ScalaDsl with EN {

  private var sparkSessionCreated: Boolean = false

  Before { _ => createSparkSession()
  }

  private def createSparkSession(): Unit = {
    if (!sparkSessionCreated) {
      TestContextProvider.createSparkSession()
      sparkSessionCreated = true
    }
  }

  def retrieveSparkSession: SparkSession = {
    TestContextProvider.getSparkSession
  }

}
