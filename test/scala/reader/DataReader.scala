package reader

import model.Data
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataReader[T](sequence: Seq[Data]) extends DataSetReader[T]  {

  override def readDataFrame(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    sequence.toDF().coalesce(1)
  }

}
