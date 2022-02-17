package config

import java.io.InputStream
import java.nio.file.{Files, Paths}
import scala.util.{Failure, Success, Try}
import org.apache.commons.io.IOUtils
import net.jcazevedo.moultingyaml._
import config.ApplicationConfigProtocol._

object ApplicationConfigReader {

  def read(resourcePath: String): Try[ApplicationConfig] = {
    Try(Files.newInputStream(Paths.get(resourcePath))) match {
      case Success(input) => Success(read(input))
      case Failure(th)    => Failure(th)
    }
  }

  private def read(inputStream: InputStream): ApplicationConfig = {
    IOUtils
      .toString(inputStream, "UTF-8")
      .parseYaml
      .convertTo[ApplicationConfig]
  }

}
