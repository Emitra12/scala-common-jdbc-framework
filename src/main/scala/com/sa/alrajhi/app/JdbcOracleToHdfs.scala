package com.sa.alrajhi.app

import com.sa.alrajhi.jdbcOracleToMapRFS.jdbcOracleToMapRFSConf
import org.apache.spark.sql.SaveMode

object JdbcOracleToHdfs extends Application[jdbcOracleToMapRFSConf] {

  def main(args: Array[String]): Unit = {

    val conf: jdbcOracleToMapRFSConf = contextProvider.configuration

    val sourceOracleDF = spark.read.format("jdbc")
      .option("driver", conf.source.driver)
      .option("url", conf.source.url)
      .option("dbtable", conf.source.query)
      .option("user", conf.source.user)
      .option("password", conf.source.password)
      .load()

    println("#--Total records extracted from OracleDB Table "+ conf.target.tableName +": "+ sourceOracleDF.count())

    val tranformedOracleDF = sourceOracleDF.createOrReplaceTempView("sourceOracleTable")

    val withPartitionColumsDF = spark.sql(conf.target.transformationQuery + " from sourceOracleTable")

    val writeParqueInMapRFSDF = withPartitionColumsDF
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("year", "month", "day")
      .option("maxRecordsPerFile", conf.target.maxRecordsPerFile)
      .parquet(conf.target.outputPath + conf.target.tableName)

    println("#--Total records written in MapR-FS for table "+ conf.target.tableName +": "+ withPartitionColumsDF.count())

    // Creating Hive external table and repair table after creation
    createHiveExternalTable.createHiveTable(sourceOracleDF)
    // Inserting data into Ingestion metadata table
    createHiveExternalTable.ingestionMetaDataAuditTable(sourceOracleDF)

  }
}