package io.iguaz.examples.utils

import scala.util.{Failure, Properties, Success, Try}

import com.typesafe.config.ConfigException.Missing
import com.typesafe.config.ConfigFactory

class Configuration(fileNameOption: Option[String] = None) {

  val config = fileNameOption.fold(
    ifEmpty = ConfigFactory.load())(
    file => ConfigFactory.load(file))

  /**
   * Lookup for a property in the following sources (accordingly)
   * 1. Environment variables (OS level, i.e. $HOME, etc.)
   * 2. Configuration file
   * 3. default value - if provided, otherwise returns null
   */
  def envConfig(key: String): Option[String] = {
    getProperty(Properties.envOrNone)(key)
  }

  /**
   * Lookup for a property in the following sources (accordingly)
   * 1. System properties (i.e. -Dprop.key=prop.value)
   * 2. Configuration file
   * 3. default value - if provided, otherwise returns null
   */
  def propConfig(key: String): Option[String] = {
    getProperty(Properties.propOrNone)(key)
  }


  private def getProperty(f: (String) => Option[String])(key: String) = {
    val sysPropKey = key.toUpperCase.replace('.', '_')

    f(sysPropKey).orElse {
      val tryOfOption = Try(config.getString(key)) match {
        case Success(value) => Success(Some(value))
        case Failure(t: Missing) => Success(None)
        case Failure(t) => Failure(t)
      }
      tryOfOption.get
    }
  }
}
