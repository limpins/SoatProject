package session

class SparkJobLauncher(sparkJobParameter: SparkJobParameter) {

  def launch(): Unit = {
    process()
    close()
  }

  private def process(): Unit = {
    sparkJobParameter.sparkProcessor
      .process(sparkJobParameter.sparkSession)
  }

  private def close(): Unit = {
    if (sparkJobParameter.closeable) {
      sparkJobParameter.sparkSession.close()
    }
  }

}

