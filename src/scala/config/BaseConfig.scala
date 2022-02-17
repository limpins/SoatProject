package config

import org.rogach.scallop.{ScallopConf, ScallopOption}

class BaseConfig(args: Seq[String]) extends ScallopConf(args) {

  val local: ScallopOption[Boolean] = toggle(
    default = None,
    descrYes = "enable to use a local Spark master"
  )

  def checkConfiguration(): Unit = {
    verify()
  }

}
