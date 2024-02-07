package dtl-aws.safir.io

object DfWriter {

  def write(df: DataFrame, partitionCols: Seq[String], tableName: String) = {
    df.repartition(partitionCols.map(col): _*)
      .write
      //.partitionBy(partitionCols: _*)
      .mode(SaveMode.Overwrite)
      .option("path", s"s3a://$tableBucket/${tableName}")
      .saveAsTable(tableName)
  }
}
