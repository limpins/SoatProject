package dtl-aws.safir.io

import java.net.{URI, URLEncoder}
import java.nio.charset.StandardCharsets
import java.time.Duration

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._
import com.socgen.cprofit.config.Configuration.WholeConfig
import scala.collection.JavaConverters._
import scala.util.Try

object FileUtil {

  def createClient(config: WholeConfig) = {
    val region = Region.US_EAST_1
    val credentials =
      AwsBasicCredentials.create(config.getClientConfig.credential.accessKey,
        config.getClientConfig.credential.secretKey)

    val configuration = ClientOverrideConfiguration
      .builder()
      .apiCallAttemptTimeout(Duration.ofMinutes(5))
      .apiCallTimeout(Duration.ofMinutes(5))
      .build()

    S3Client
      .builder()
      .credentialsProvider(StaticCredentialsProvider.create(credentials))
      .region(region)
      .overrideConfiguration(configuration)
      .endpointOverride(URI.create(config.getClientConfig.uri))
      .build()

  }

  def listBucketObjects(bucketName: String)(implicit client: S3Client) = {
    val listObjects = ListObjectsRequest.builder.bucket(bucketName).build
    val res = client.listObjects(listObjects)
    res.contents.asScala.toList.map(_.key)
  }

  def copyBucketObject(fromBucket: String, objectKey: String, toBucket: String)(implicit client: S3Client) = {

    val tryEncodedUrl = Try {
      URLEncoder.encode(fromBucket + "/" + objectKey, StandardCharsets.UTF_8.toString)
    }

    val tryClient = tryEncodedUrl.map {
      case encodedUrl =>
        CopyObjectRequest.builder
          .copySource(encodedUrl)
          .destinationBucket(toBucket)
          .destinationKey(objectKey)
          .build
    }

    tryClient.toOption.map(req => {
      val copyRes = client.copyObject(req)
      copyRes.copyObjectResult().toString
    })
  }

  def deleteBucketObjects(bucketName: String, objectkey: String)(implicit client: S3Client) = {

    val toDelete = Seq(ObjectIdentifier.builder.key(objectkey).build).asJava

    Try {
      val deleteRequest =
        DeleteObjectsRequest.builder.bucket(bucketName).delete(Delete.builder.objects(toDelete).build).build
      client.deleteObjects(deleteRequest)
    }.toOption

  }

}
