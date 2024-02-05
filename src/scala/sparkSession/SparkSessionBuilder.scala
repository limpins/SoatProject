package session

import com.socgen.cprofit.dtl.common.config.BaseConfig
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkSessionBuilder {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def createSparkSession(applicationName: String,
                         baseConfig: BaseConfig,
                         sparkProperties: Option[SparkProperties] = None): SparkSession = {
    getDefaultSparkSessionBuilder(applicationName, baseConfig, sparkProperties)
      .config("spark.sql.broadcastTimeout", "36000")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("spark.sql.parquet.cacheMetadata", "false")
      .getOrCreate()
  }

  private def getDefaultSparkSessionBuilder(applicationName: String,
                                            baseConfig: BaseConfig,
                                            sparkProperties: Option[SparkProperties]): SparkSession.Builder = {

    var sparkSessionBuilder = SparkSession
      .builder()
      .appName(applicationName)

    if (isLocal(baseConfig.local.toOption)) {
      sparkSessionBuilder = addLocalConfiguration(sparkSessionBuilder)
      authenticateToKerberos()
    }

    sparkSessionBuilder = sparkProperties
      .flatMap(
        prop => Some(addSparkProperties(sparkSessionBuilder, prop))
      )
      .getOrElse(sparkSessionBuilder)

    sparkSessionBuilder
  }

  def addSparkProperties(sparkSessionBuilder: SparkSession.Builder,
                         sparkProperties: SparkProperties): SparkSession.Builder = {
    val builder: SparkSession.Builder = sparkSessionBuilder
    for ((key, value) <- sparkProperties.properties) {
      logger.info(s"adding property $key with value $value to spark configuration")
      builder.config(key, value)
    }
    builder
  }

  private def isLocal(localOption: Option[Boolean]): Boolean = {
    localOption.getOrElse(false)
  }

  private def addLocalConfiguration(sparkSessionBuilder: SparkSession.Builder): SparkSession.Builder = {
    sparkSessionBuilder.master("local[2]")
    sparkSessionBuilder.config("spark.sql.shuffle.partitions", "1")
    sparkSessionBuilder.config("spark.default.parallelism", "2")
  }

  private def authenticateToKerberos(): Unit = {
    val userName = System.getProperty("user.name")
    val userHome = System.getProperty("user.home")
    val userKeyTabPath = s"$userHome/$userName.keytab"
    UserGroupInformation.loginUserFromKeytab(userName, userKeyTabPath)
  }

}
