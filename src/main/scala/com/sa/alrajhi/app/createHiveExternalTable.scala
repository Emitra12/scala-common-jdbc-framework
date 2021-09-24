package com.sa.alrajhi.app

import com.sa.alrajhi.jdbcOracleToMapRFS.jdbcOracleToMapRFSConf
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._


object createHiveExternalTable extends Application[jdbcOracleToMapRFSConf] {

      val conf: jdbcOracleToMapRFSConf = contextProvider.configuration

   def createHiveTable(sourceDF: DataFrame): DataFrame = {

      val columns = sourceDF.schema.toDDL
      val tableName = conf.target.tableName
      val tableLocationPath = conf.target.outputPath
      val partitionedByExpression = "(year int, month int, day int)"
      val rowFormatSerdeOrDefault = "ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'"
      val inputFormatSerde = "STORED AS INPUTFORMAT 'org.apache.hudi.hadoop.HoodieParquetInputFormat'"
      val outPutFormatSerde = "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'"

      val createTableStatement = s"CREATE EXTERNAL TABLE IF NOT EXISTS $tableName ($columns) " +
        s"PARTITIONED BY $partitionedByExpression $rowFormatSerdeOrDefault " +
        s"$inputFormatSerde " +
        s"$outPutFormatSerde LOCATION '$tableLocationPath'"

      sourceDF.printSchema()
      // spark.sql(createTableStatement)
      println(createTableStatement)

      val repairStatement = s"MSCK REPAIR TABLE ${conf.source.schema_new}"
      //       spark.sql(repairStatement)
      println(repairStatement)

      spark.emptyDataFrame
   }

  def ingestionMetaDataAuditTable(sourceMetaDataDF: DataFrame): DataFrame = {

    val totalRecCount = sourceMetaDataDF.count()
    val arb_ingestion_time = date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss")
    val TableName = conf.target.tableName
    val schemaName =  conf.source.schema_new.split("\\.[^\\.]*$").mkString

    sourceMetaDataDF
      .withColumn("schemaName",lit(schemaName))
      .withColumn("tablename", lit(TableName))
      .withColumn("tot_ingestion_rec_cnt",lit(totalRecCount))
      .withColumn("last_batch_ingestion_time",lit(arb_ingestion_time))
      .select(
        col("schemaName"),
        col("tablename"),
        col("tot_ingestion_rec_cnt"),
        col("last_batch_ingestion_time")
      ).show(false)

//      .write
//      .mode(SaveMode.Overwrite)
//      .partitionBy("year", "month", "day")
//      .option("maxRecordsPerFile", conf.target.maxRecordsPerFile)
//      .parquet(conf.target.outputPath + conf.target.tableName)

    spark.emptyDataFrame

  }
}
