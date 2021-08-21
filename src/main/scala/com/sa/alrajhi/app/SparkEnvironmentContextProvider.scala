package com.sa.alrajhi.app

import org.apache.spark.sql.SparkSession

class SparkEnvironmentContextProvider[+A: Manifest](implicit spark: SparkSession) extends StreamContextProvider[A] {

  override val contextPrefix: Option[String] = Some("spark")

  override def hasAttribute(attribute: String): Boolean = spark.conf.getOption(attribute).isDefined

  override def getAttribute(attribute: String): String = spark.conf.get(attribute)
}
