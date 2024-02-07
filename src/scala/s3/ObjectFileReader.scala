package dtl-aws.safir.io

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import software.amazon.awssdk.services.s3.S3Client

object ObjectFileReader {

  case class SafirReader(fileName: String, df: DataFrame)

  def getSafirReaders(schema: StructType, bucketName: String)(implicit sparkSession: SparkSession, client: S3Client) = {
    FileUtil.listBucketObjects(bucketName).map(getSafirReader(_, bucketName, schema))
  }

  def getSafirReader(objectKey: String, bucketName: String, schema: StructType)(implicit sparkSession: SparkSession) = {

    val safirOptions = Map(
      "inferSchema" -> "false",
      "header" -> "true",
      "delimiter" -> ";",
      "mode" -> "permissive",
      "escape" -> "\""
    )

    SafirReader(
      objectKey,
      sparkSession.read
        .options(safirOptions)
        .schema(schema)
        .csv(s"s3a://${bucketName}/${objectKey}")
    )
  }

}