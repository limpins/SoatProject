package writer

import model.Data
import org.apache.spark.sql.Dataset

class ResultWriter[T] extends DataSetWriter[T] {

  var result: Seq[T] = Nil

   override def write(dataSet: Dataset[T]): Unit = {
    result = dataSet
      .coalesce(1)
      .collect()
  }

  def clear(): Unit = {
    result = Seq.empty
  }

}
