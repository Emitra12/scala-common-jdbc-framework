package com.sa.alrajhi.app

import net.liftweb.json.Formats
import org.apache.spark.sql.SparkSession
import com.sa.alrajhi.Configuration


abstract class EnvironmentContextProvider[+A : Manifest] extends ContextProvider {

  implicit lazy val spark: SparkSession = SparkSession.builder().getOrCreate()
  implicit val formats: Formats = net.liftweb.json.DefaultFormats
  lazy val hasConfiguration: Boolean = hasAttribute(getAttributeName(Configuration.CONFIGURATION_LOCATION_KEY))
  lazy val configuration: A = getConfiguration

  protected lazy val configString: String = getAttribute(getAttributeName(Configuration.CONFIGURATION_LOCATION_KEY))

  @throws(classOf[RuntimeException])
  protected def getConfiguration: A = {
    if(hasConfiguration) {
      val path = configString.trim

      // this is clearly json document
      if (path.startsWith("{") && path.endsWith("}")) {
        Configuration.fromJson[A](path)
      } else {
        Configuration.fromJsonFile[A](path)
      }
    } else {
      throw new IllegalArgumentException("No configuration given")
    }
  }


  def withConfiguration[C: Manifest](newConfiguration: C): EnvironmentContextProvider[C] = {
    val sourceContext = this
    val sourceContextPrefix = sourceContext.contextPrefix
    lazy val sourceContextConfigString = sourceContext.configString

    new EnvironmentContextProvider[C] {
      override def hasAttribute(attribute: String): Boolean = sourceContext.hasAttribute(attribute)
      override def getAttribute(attribute: String): String = sourceContext.getAttribute(attribute)
      override val contextPrefix: Option[String] = sourceContextPrefix

      // environment context
      override lazy val hasConfiguration: Boolean = true
      override lazy val configuration: C = newConfiguration
      override lazy val configString: String = sourceContextConfigString
    }
  }
}
