package com.sa.alrajhi.app

import org.apache.spark.sql.SparkSession

abstract class Application[A: Manifest] {
  implicit lazy val spark: SparkSession = SparkSession.builder().getOrCreate()
  implicit lazy val contextProvider: StreamContextProvider[A] = new SparkEnvironmentContextProvider[A]
}
