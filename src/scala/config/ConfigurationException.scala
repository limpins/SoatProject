package config

final case class ConfigurationException(private val message: String = "", private val cause: Throwable = None.orNull)
  extends RuntimeException(message, cause) {}
