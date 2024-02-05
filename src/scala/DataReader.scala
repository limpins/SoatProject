package correction

import com.socgen.cprofit.correctionconsumer.Queries._
import com.socgen.cprofit.dtl.common.config.yaml.{JdbcDatabase, ParquetConfig}
import com.socgen.cprofit.dtl.common.spark.reader.DataSetReader
import com.socgen.cprofit.dtl.common.spark.reader.jdbc.JdbcReader
import com.socgen.cprofit.dtl.common.spark.reader.parquet.ParquetReader
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object DataReader {

  val referentialDB = "referential"
  val lakeDB = "lake"
  val appDB = "app"
  val referentialBucket = "ref"
  val esBucket = "es"

  def getCurrentDealReader(s3Config: ParquetConfig): DataSetReader[Row] = {
    val dimDealBucketPath =
      s3Config.getPath(referentialBucket,"dimDeal")
    new ParquetReader[Row](dimDealBucketPath)
  }

  def getHistoricalDealReader(s3Config: ParquetConfig): DataSetReader[Row] = {
    val dimHistDealBucketPath =
      s3Config.getPath(referentialBucket,"historicalDeal")
    new ParquetReader[Row](dimHistDealBucketPath)
  }

  def getApplicationReader(s3Config: ParquetConfig): DataSetReader[Row] = {
    val dimHistDealBucketPath =
      s3Config.getPath(referentialBucket,"application")
    new ParquetReader[Row](dimHistDealBucketPath)
  }

  def getCorrectionBpSharingEsReader(s3Config: ParquetConfig): DataSetReader[Row] = {
    val dimCorrectionBpSharingBucketPath =
      s3Config.getPath(esBucket,"correctionBpSharingEs")
    new ParquetReader[Row](dimCorrectionBpSharingBucketPath)
  }

  def getCommercialClientReader(s3Config: ParquetConfig): DataSetReader[Row] = {
    val dimCommercialBucketPath =
      s3Config.getPath(referentialBucket,"dimCommercial")
    new ParquetReader[Row](dimCommercialBucketPath)
  }

  def getDimEntityReader(s3Config: ParquetConfig): DataSetReader[Row] = {
    val dimEntityBucketPath =
      s3Config.getPath(referentialBucket,"dimEntity")
    new ParquetReader[Row](dimEntityBucketPath)
  }

  def getDimAgencyReader(s3Config: ParquetConfig): DataSetReader[Row] = {
    val dimAgencyPath =
      s3Config.getPath(referentialBucket,"dimAgency")
    new ParquetReader[Row](dimAgencyPath)
  }

  def getFpvReader(s3Config: ParquetConfig): DataSetReader[Row] = {
    val dimFpvPath =
      s3Config.getPath(referentialBucket,"dimFpv")
    new ParquetReader[Row](dimFpvPath)
  }

  def getProductReader(s3Config: ParquetConfig): DataSetReader[Row] = {
    val dimProductPath =
      s3Config.getPath(referentialBucket,"dimProduct")
    new ParquetReader[Row](dimProductPath)
  }

  def getBpFinalClientReader(s3Config: ParquetConfig): DataSetReader[Row] = {
    val dimBpFinalClientPath =
      s3Config.getPath(referentialBucket,"bpFinalClient")
    new ParquetReader[Row](dimBpFinalClientPath)
  }

  def getLegalClientReader(config: JdbcDatabase)(implicit sparkSession: SparkSession): DataFrame = {
    import com.socgen.cprofit.dtl.common.Implicits._
    JdbcReader(config)
      .read(clientQuery(config.getTable("client")))
      .toLowercaseDF
      .castToLongCols("client_id", "ec_id")
  }

  def getCorrectionReader(config: JdbcDatabase)(implicit sparkSession: SparkSession): DataFrame = {
    import com.socgen.cprofit.dtl.common.Implicits._
    JdbcReader(config)
      .read(correctionQuery(config.getTable("correction")))
      .toLowercaseDF
      .castToLongCols("correction_id")
  }

  def getCorrectionBpSharingReader(config: JdbcDatabase)(implicit sparkSession: SparkSession): DataFrame = {
    import com.socgen.cprofit.dtl.common.Implicits._
    JdbcReader(config)
      .read(correctionBpSharingQuery(config.getTable("correctionSharing")))
      .toLowercaseDF
      .castToLongCols("start_date", "end_date", "deal_id", "bp_client_id", "source_app_id", "correction_id")
      .castToDecimalCols("sharing_percentage")
  }

  def getCorrectionsReader(config: JdbcDatabase)(implicit sparkSession: SparkSession): DataFrame = {
    import com.socgen.cprofit.dtl.common.Implicits._
    JdbcReader(config)
      .read(correctionsQuery(config.getTable("corrections")))
      .toLowercaseDF
      .castToLongCols("correction_id")
  }

  def getBpSharingsReader(config: JdbcDatabase)(implicit sparkSession: SparkSession): DataFrame = {
    import com.socgen.cprofit.dtl.common.Implicits._
    JdbcReader(config)
      .read(bpSharingsQuery(config.getTable("bpSharings")))
      .toLowercaseDF
      .castToLongCols("correction_id")
      .castToDecimalCols("share")
  }

}