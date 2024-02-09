package dtl-aws.config

import java.io.InputStream

import com.socgen.cprofit.config.ApplicationConfigProtocol._
import org.apache.commons.io.IOUtils

import scala.util.{Failure, Success, Try}

object Configuration {

  def readResource(resourcePath: String): Try[WholeConfig] = {
    Try(getClass.getResourceAsStream(resourcePath)) match {
      case Success(input) => Success(read(input))
      case Failure(th)    => Failure(th)
    }

  }

  private def read(inputStream: InputStream): WholeConfig = {
    import net.jcazevedo.moultingyaml._
    IOUtils
      .toString(inputStream, "UTF-8")
      .parseYaml
      .convertTo[WholeConfig]
  }

  case class ParquetConfig(paths: Map[String, ParquetLocation]) {
    def getPath(rootLabel: String): ParquetLocation = {
      paths.getOrElse(rootLabel, throw new IllegalArgumentException)
    }
  }

  case class ParquetLocation(rootPath: String, data: Map[String, String]) {
    def getPath(key: String): String = {
      s"s3a://$rootPath/${data.getOrElse(key, throw new IllegalArgumentException)}"
    }
  }

  final case class WholeConfig(spark: Option[SparkConfig],
                               s3Client: Option[AwsClientConfig],
                               s3Request: Option[AwsRequest],
                               s3CopyRequest: Option[AwsCopyRequest],
                               parquet: Option[ParquetConfig],
                               jdbc: Option[JdbcConfig],
                               mapping: Option[MappingConfig]) {

    def getMappingConfig: MappingConfig = {
      mapping.getOrElse(throw new IllegalArgumentException)
    }

    def getSpark() = {
      spark.getOrElse(throw new IllegalArgumentException)
    }

    def getClientConfig() = {
      s3Client.getOrElse(throw new IllegalArgumentException)
    }

    def getRequestConfig() = {
      s3Request.getOrElse(throw new IllegalArgumentException)
    }

    def getCopyRequestConfig() = {
      s3CopyRequest.getOrElse(throw new IllegalArgumentException)
    }

    def getParquetConfig: ParquetConfig = {
      parquet.getOrElse(throw new IllegalArgumentException)
    }

    def getJdbcConfig: JdbcConfig = {
      jdbc.getOrElse(throw new IllegalArgumentException)
    }

  }

  case class JdbcConfig(databases: Map[String, JdbcDatabase]) {

    def getDatabase(databaseLabel: String): JdbcDatabase = {
      databases.getOrElse(databaseLabel, throw new IllegalArgumentException)
    }

  }

  case class JdbcDatabase(connection: JdbcConnection, tables: Map[String, String]) {

    def getTable(key: String): String = {
      tables.getOrElse(key, throw new IllegalArgumentException)
    }

    def getTableFullName(tableLabel: String): String = {
      s"${connection.username}.${getTable(tableLabel)}"
    }

    def connectionName() = connection.getConnection()

    def username() = connection.username

    def pass() = connection.pass

  }

  case class JdbcConnection(dbType: Option[String],
                            host: String,
                            port: String,
                            service: String,
                            username: String,
                            pass: String) {

    def getConnection() = {
      s"jdbc:oracle:thin:@$host:$port/$service"
    }
  }

  case class MappingConfig(keys: Map[String, String]) {

    def getKey(key: String): String = {
      keys.getOrElse(key, throw new IllegalArgumentException)
    }

  }

  final case class AwsClientConfig(uri: String, credential: AwsCredentialConfig)

  final case class AwsCredentialConfig(accessKey: String, secretKey: String)

  final case class SparkConfig(appName: String, master: String, sparkConfs: Map[String, String])

  final case class AwsCopyRequest(fromBucket: String, toBucket: String)

  final case class AwsRequest(bucket: String)
}