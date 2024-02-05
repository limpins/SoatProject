package session

class SparkProperties {
  var properties: Map[String, String] = Map.empty

  def setShufflePartition(shufflePartitions: String) {
    properties = properties + ("spark.sql.shuffle.partitions" -> shufflePartitions)
  }
}