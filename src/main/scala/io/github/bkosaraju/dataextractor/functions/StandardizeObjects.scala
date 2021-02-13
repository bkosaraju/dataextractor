/*
 * Copyright (C) 2019-2021 bkosaraju
 * All Rights Reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *   http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package io.github.bkosaraju.dataextractor.functions

import io.github.bkosaraju.utils.spark.AmendDwsCols
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, current_timestamp, lit, when}

trait StandardizeObjects extends Session with AmendDwsCols {
  def standardizeColumnNames(optDF: DataFrame): DataFrame = {
    try {
      optDF.schema.map(_.name).foldLeft(optDF)(
        (df, col) => df.withColumnRenamed(
          col,
          col.replaceAll("[ ,;{}()\\n\\t=.]", "_")
        )
      )
    } catch {
      case e: Exception => {
        logger.error("Unable to cleance column names in dataframe", e)
        throw e
      }
    }
  }

  def sanitizeNulls(optDF: DataFrame): DataFrame = {
    try {
      val tgtDF = ammendDefaultAuditCols(standardizeColumnNames(optDF))

      tgtDF.schema
        .filter(x => x.dataType.typeName.equalsIgnoreCase("string"))
        .map(_.name).toList
        .foldLeft(tgtDF)((sDF, clmn) => {
          sDF.withColumn(
            clmn,
            when(col(clmn) === "null", lit(null)).otherwise(col(clmn)))
        }
        )
    } catch {
      case e: Exception => {
        logger.error("Unable to Process dataframe generated with extracted data from source", e)
        throw e
      }
    }
  }

    def ammendDefaultAuditCols(stgDF: DataFrame): DataFrame = {
      try {
        stgDF.genHash(stgDF.schema.names, "src_hash")
          .withColumn(
            "extract_dttm", lit(current_timestamp())
          )
      } catch {
        case e: Exception => logger.error("Unabel to Add src_hash and extract_dttm for given dataframe")
          throw e
      }
    }
}
