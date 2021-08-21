package com.sa.alrajhi

import net.liftweb.json.Formats
import com.sa.alrajhi.Helper.autoCloseSource

package object Configuration {

  implicit val formats: Formats = net.liftweb.json.DefaultFormats

  val CONFIGURATION_LOCATION_KEY: String = "configurationSource"

  def fromJson[A: Manifest](content: String)(implicit formats: Formats): A = {

    import net.liftweb.json.parse

    parse(content).extract[A]
  }

  /***
   * creates a new Environment based off of a json file on classpath in config/<tenant>.json
   *
   * @return a new environment
   */

  private val classpathPrefix = "classpath:"

  def fromJsonFileForTenant[A: Manifest](tenant: String)(implicit formats: Formats): A= {
    fromJsonFile[A](
      s"${classpathPrefix}config/$tenant.json"
    )
  }

  /***
   * creates a new Environment based off of a json file path
   *
   * @param path the json document absolute file path
   * @return a new environment
   */
  def fromJsonFile[A: Manifest](path: String)(implicit formats: Formats): A = {
    if (path.endsWith(".json")) {
      if(path.startsWith(classpathPrefix)) {
        val configFromClassPath = getClass.getClassLoader.getResourceAsStream(path.substring(classpathPrefix.length))
        if(configFromClassPath == null) {
          throw new IllegalArgumentException(s"No config file on classpath at $path")
        }

        fromJson[A](
          autoCloseSource(
            scala.io.Source.fromInputStream(configFromClassPath)
          )(source => source.mkString)
        )
      } else {
        fromJson[A](
          autoCloseSource(
            scala.io.Source.fromFile(path)
          )(source => source.mkString)
        )
      }
    } else {
      throw new IllegalArgumentException("not supported format")
    }
  }

}

