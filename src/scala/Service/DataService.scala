package Service

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object DataService {

  def map(dataDF: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {

    import sparkSession.implicits._
    val w = Window.partitionBy('key, 'value)

    val res = dataDF
      .withColumn("count", functions.count('value) over w)
      .withColumn("max", functions.max('count) over w)
      .where('count === 'max)
      .drop('max)

    res.show()
    res
  }

}
