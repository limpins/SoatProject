import cucumber.api.CucumberOptions
import cucumber.api.junit.Cucumber
import org.junit.runner.RunWith

@RunWith(classOf[Cucumber])
@CucumberOptions(
  features = Array("test/resources/Project.feature"),
  tags = Array("not @Wip"),
  glue = Array("stepdefs"),
  plugin = Array("pretty", "json:target/cucumber/data.json")
)
class DataRunnerTest
