package config

import org.rogach.scallop.ScallopOption

class ProjectConfig(args: Seq[String]) extends BaseConfig(args) {

  val applicationConfigPath: ScallopOption[String] =
    opt[String](required = false, descr = "application configuration path")

  val workflowName: ScallopOption[String] =
    opt[String](
      descr = "oozie workflow name"
    )

  val userName: ScallopOption[String] =
    opt[String](
      descr = "oozie workflow user name"
    )
}
