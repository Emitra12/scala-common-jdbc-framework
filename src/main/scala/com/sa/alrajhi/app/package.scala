package com.sa.alrajhi

import org.apache.spark.sql.types.StructType

package object jdbcOracleToMapRFS {

  case class Source(
                     url: String,
                     driver: String,
                     user: String,
                     password: String,
                     query: String,
                     schema: Option[StructType]
                   )

  case class Target(
                     outputPath: String,
                     format: String,
                     transformationQuery: String,
                     maxRecordsPerFile: Int,
                     tableName: String
                   )

  case class jdbcOracleToMapRFSConf (source: Source, target: Target)
}
