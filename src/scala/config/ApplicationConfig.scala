package config

case class ApplicationConfig(files: Option[FilesConfig],
                             spark: Option[SparkConfig],
                             s3Client: Option[AwsClientConfig],
                             s3Request: Option[AwsRequest],
                             s3CopyRequest: Option[AwsCopyRequest],
                             parquet: Option[ParquetConfig],
                             avro: Option[AvroConfig],
                             tables: Option[TablesConfig],
                             jdbc: Option[JdbcConfig],
                             mapping: Option[MappingConfig]) {

  def getClientConfig() = {
    s3Client.getOrElse(throw new IllegalArgumentException)
  }

  def getRequestConfig() = {
    s3Request.getOrElse(throw new IllegalArgumentException)
  }

  def getCopyRequestConfig() = {
    s3CopyRequest.getOrElse(throw new IllegalArgumentException)
  }

  def getFilesConfig: FilesConfig = {
    files.getOrElse(throw ConfigurationException("file config does not exist"))
  }

  def getParquetConfig: ParquetConfig = {
    parquet.getOrElse(throw ConfigurationException("parquet config does not exist"))
  }

  def getSpark() = {
    spark.getOrElse(throw new IllegalArgumentException)
  }

  def getAvroConfig: AvroConfig = {
    avro.getOrElse(throw ConfigurationException("avro config does not exist"))
  }

  def getTableConfig: TablesConfig = {
    tables.getOrElse(throw ConfigurationException("hive config does not exist"))
  }

  def getJdbcConfig: JdbcConfig = {
    jdbc.getOrElse(throw ConfigurationException("jdbc config does not exist"))
  }

  def getMappingConfig: MappingConfig = {
    mapping.getOrElse(throw ConfigurationException("mapping config does not exist"))
  }

}

final case class AwsClientConfig(uri: String, credential: AwsCredentialConfig)

final case class AwsCredentialConfig(accessKey: String, secretKey: String)

final case class SparkConfig(appName: String, master: String, sparkConfs: Map[String, String])

final case class AwsCopyRequest(fromBucket: String, toBucket: String)

final case class AwsRequest(bucket: String)

case class FilesConfig(paths: Map[String, FilesLocation]) {
  def getPath(rootLabel: String, locationLabel: String): String = {
    paths.getOrElse(rootLabel, throw ConfigurationException(s"$rootLabel does not exist")).getPath(locationLabel)
  }
}

case class FilesLocation(rootPath: String, data: Map[String, String]) {
  def getPath(key: String): String = {
    s"$rootPath/${data.getOrElse(key, throw ConfigurationException(s"path $key does not exist"))}"
  }
}

case class ParquetConfig(paths: Map[String, ParquetLocation]) {
  def getPath(rootLabel: String, locationLabel: String): String = {
    paths.getOrElse(rootLabel, throw ConfigurationException(s"$rootLabel does not exist")).getPath(locationLabel)
  }
}

case class ParquetLocation(rootPath: String, data: Map[String, String]) {
  def getPath(key: String): String = {
    s"s3a://$rootPath/${data.getOrElse(key, throw ConfigurationException(s"path $key does not exist"))}"
  }
}

case class AvroConfig(paths: Map[String, AvroLocation]) {
  def getPath(rootLabel: String, locationLabel: String): String = {
    paths.getOrElse(rootLabel, throw ConfigurationException(s"$rootLabel does not exist")).getPath(locationLabel)
  }
}

case class AvroLocation(rootPath: String, data: Map[String, String]) {
  def getPath(key: String): String = {
    s"$rootPath/${data.getOrElse(key, throw ConfigurationException(s"path $key does not exist"))}"
  }
}

case class HiveConfig(databases: Map[String, HiveDatabase]) {

  def getDatabase(databaseLabel: String): HiveDatabase = {
    databases.getOrElse(databaseLabel, throw ConfigurationException(s"hive database $databaseLabel does not exist"))
  }

  def getTableFullName(databaseLabel: String, tableLabel: String): String = {
    getDatabase(databaseLabel).getTableFullName(tableLabel)
  }
}

case class HiveDatabase(dbName: String, tables: Map[String, String]) {

  def getTable(key: String): String = {
    tables.getOrElse(key, throw ConfigurationException(s"hive table $key does not exist"))
  }

  def getTableFullName(key: String): String = {
    s"$dbName.${getTable(key)}"
  }
}

case class TablesConfig(configs: Map[String, TableConfig]) {

  def getTable(tableLabel: String): TableConfig = {
    configs.getOrElse(tableLabel, throw ConfigurationException(s"table config $tableLabel does not exist"))
  }
}

case class TableConfig(dbName: String, tableName: String, path: Option[String]) {

  def getTableFullName(key: String): String = {
    s"$dbName.$tableName"
  }
}

case class JdbcConfig(databases: Map[String, JdbcDatabase]) {

  def getDatabase(databaseLabel: String): JdbcDatabase = {
    databases.getOrElse(databaseLabel, throw ConfigurationException(s"jdbc database $databaseLabel does not exist"))
  }

}

case class JdbcDatabase(connection: JdbcConnection, tables: Map[String, String]) {

  def getTable(key: String): String = {
    tables.getOrElse(key, throw ConfigurationException(s"jdbc table $key does not exist"))
  }

  def getType(): String = {
    connection.dbType.getOrElse("oracle")
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

    dbType match {
      case Some(typ) if typ == "postgresql" => s"jdbc:$typ://$host:$port/$service"
      case _                                => s"jdbc:oracle:thin:@$host:$port/$service"
    }
  }
}

case class MappingConfig(keys: Map[String, String]) {

  def getKey(key: String): String = {
    keys.getOrElse(key, throw ConfigurationException(s"key $key does not exist"))
  }

}
