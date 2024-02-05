package writer.jdbc

import com.socgen.cprofit.dtl.common.config.yaml.JdbcDatabase
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

case class JdbcWriter(config: JdbcDatabase)(implicit sparkSession: SparkSession) {

  def buildProps(truncate: String = "true") = {
    val props = new java.util.Properties
    props.setProperty("driver", "org.postgresql.Driver")
    props.setProperty("user", config.username)
    props.setProperty("password", config.pass)
    props.setProperty("encoding", "UTF-8")
    props.setProperty("truncate", truncate)
    props.setProperty("characterEncoding", "UTF-8")
    props.setProperty("batchsize", "10000")
    props
  }

  def write(df: DataFrame, destTable: String, truncate: String = "true") = {
    df.write.mode(SaveMode.Overwrite).jdbc(config.connectionName, destTable, buildProps(truncate))
  }

}