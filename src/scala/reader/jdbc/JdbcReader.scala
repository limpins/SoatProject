package reader.jdbc

import com.socgen.cprofit.dtl.common.config.yaml.JdbcDatabase
import org.apache.hadoop.security.alias.CredentialProviderFactory
import org.apache.spark.sql.SparkSession

case class JdbcReader(config: JdbcDatabase)(implicit sparkSession: SparkSession) {

  def buildProps = {
    val props = new java.util.Properties
    props.setProperty("driver", "oracle.jdbc.OracleDriver")
    props.setProperty("user", config.username)
    props.setProperty("password", config.pass)
    props.setProperty("encoding", "UTF-8")
    props.setProperty("characterEncoding", "UTF-8")
    props.setProperty("fetchsize", "1000")
    props
  }

  def buildPostgresProps = {
    val props = new java.util.Properties
    props.setProperty("driver", "org.postgresql.Driver")
    props.setProperty("user", config.username)
    props.setProperty("password", config.pass)
    props.setProperty("encoding", "UTF-8")
    props.setProperty("characterEncoding", "UTF-8")
    props.setProperty("fetchsize", "1000")
    props
  }

  def read(query: String, predicatesOp: Option[Array[String]] = None) = {
    val props = if (config.getType() == "postgresql") buildPostgresProps else buildProps
    predicatesOp.fold(sparkSession.read.jdbc(config.connectionName, query, props)) {
      case predicates => sparkSession.read.jdbc(config.connectionName, query, predicates, props)
    }
  }

