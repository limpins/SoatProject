package dtl-aws.config

import com.socgen.cprofit.config.Configuration._
import net.jcazevedo.moultingyaml._

object ApplicationConfigProtocol extends DefaultYamlProtocol {
  implicit val sparkConfig = yamlFormat3(SparkConfig)

  implicit val awsCredentialConfig = yamlFormat2(AwsCredentialConfig)
  implicit val awsClientConfig = yamlFormat2(AwsClientConfig)

  implicit val awsCopyRequest = yamlFormat2(AwsCopyRequest)
  implicit val awsRequest = yamlFormat1(AwsRequest)

  implicit val parquetLocationFormat = yamlFormat2(ParquetLocation)
  implicit val parquetConfFormat = yamlFormat1(ParquetConfig)

  implicit val jdbcConnectionFormat = yamlFormat6(JdbcConnection)
  implicit val jdbcDatabaseFormat = yamlFormat2(JdbcDatabase)
  implicit val jdbcConfFormat = yamlFormat1(JdbcConfig)
  implicit val mappingConfFormat = yamlFormat1(MappingConfig)

  implicit val wholeConfig = yamlFormat7(WholeConfig)
}
