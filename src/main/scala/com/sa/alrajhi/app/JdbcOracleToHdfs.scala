package com.sa.alrajhi.app

import com.sa.alrajhi.jdbcOracleToMapRFS.jdbcOracleToMapRFSConf

object JdbcOracleToHdfs extends Application[jdbcOracleToMapRFSConf] {

  def main(args: Array[String]): Unit = {

    val conf: jdbcOracleToMapRFSConf = contextProvider.configuration

    println("#--Configuraion parameters value from json config file--#")

        println(conf.source.driver)
        println(conf.source.url)
        println(conf.source.query)
        println(conf.target.outputPath)
        println(conf.target.format)
        println(conf.target.maxRecordsPerFile)
        println(conf.target.tableName)
        println(conf.target.transformationQuery)

    val sourceOracleDF = spark.read.format("jdbc")
      .option("driver", conf.source.driver)
      .option("url", conf.source.url)
      .option("dbtable", conf.source.query)
      .option("user", conf.source.user)
      .option("password", conf.source.password)
      .load()
      .cache()

    val tranformedOracleDF = sourceOracleDF.createOrReplaceTempView("sourceOracleTable")

    val dfWithPartitionColums = spark.sql(conf.target.transformationQuery + " from sourceOracleTable")

    val targetDFWithParquet = dfWithPartitionColums
      .write
      .partitionBy("year", "month", "day")
      .option("maxRecordsPerFile", conf.target.maxRecordsPerFile)
      .parquet(conf.target.outputPath + conf.target.tableName)

    val sourceOracleCountDF = sourceOracleDF.count()

    println("#--Total records imported in HPE MapR-FS from OracleDB Table "+ conf.target.tableName +": "+ sourceOracleCountDF)

  }
}