package config

import net.jcazevedo.moultingyaml.DefaultYamlProtocol._

object ApplicationConfigProtocol {

  implicit val fileLocationFormat = yamlFormat2(FilesLocation)
  implicit val fileConfFormat = yamlFormat1(FilesConfig)

  implicit val sparkConfig = yamlFormat3(SparkConfig)

  implicit val awsCredentialConfig = yamlFormat2(AwsCredentialConfig)
  implicit val awsClientConfig = yamlFormat2(AwsClientConfig)

  implicit val awsCopyRequest = yamlFormat2(AwsCopyRequest)
  implicit val awsRequest = yamlFormat1(AwsRequest)

  implicit val parquetLocationFormat = yamlFormat2(ParquetLocation)
  implicit val parquetConfFormat = yamlFormat1(ParquetConfig)

  implicit val avroLocationFormat = yamlFormat2(AvroLocation)
  implicit val avroConfFormat = yamlFormat1(AvroConfig)

  implicit val jdbcConnectionFormat = yamlFormat6(JdbcConnection)
  implicit val jdbcDatabaseFormat = yamlFormat2(JdbcDatabase)
  implicit val jdbcConfFormat = yamlFormat1(JdbcConfig)
  implicit val mappingConfFormat = yamlFormat1(MappingConfig)

  implicit val tableConfFormat = yamlFormat3(TableConfig)
  implicit val tablesConfFormat = yamlFormat1(TablesConfig)

  implicit val applicationConfig = yamlFormat10(ApplicationConfig)

}
