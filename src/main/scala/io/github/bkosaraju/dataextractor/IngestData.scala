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
package io.github.bkosaraju.dataextractor

import java.util.InputMismatchException
import java.util.concurrent.ForkJoinPool
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ForkJoinTaskSupport

class IngestData extends AppFunctions {
  /**
   * Entry method to Ingest the data into URI from JDBC
   * This method is a higher order method which invoke Ingestion functions Such as decrypting the encrypted User credentials and calling the loader functions etc..
   *
   * @param srcConfig - configuration to load into Local/cloud/HDFS storage.
   * @return Unit
   */
  var edwsCols: String = _

  def ingestData(srcConfig: Map[String, String]): Unit = {
    try {
      val localConfig: collection.mutable.Map[String, String] =
        collection.mutable.Map[String, String]() ++
          srcConfig ++
          srcConfig.getOrElse("readerOptions", "").replaceAll("\"", "").stringToMap ++
          srcConfig.getOrElse("dwsVars", "").stringToMap

      if (localConfig.contains("extraDWSColumns")) {
        edwsCols = localConfig.getOrElse("extraDWSColumns", "")
        localConfig.keySet.toList.foreach(r => edwsCols = edwsCols.replaceAll(s"#${r}#", localConfig.getOrElse(r, "")))
        localConfig.put("dwsVars",
          Array(localConfig.getOrElse("dwsVars", ""), edwsCols).filter(_.nonEmpty).mkString(",")
        )
      }
      val config = localConfig.toMap
      var listedTables: collection.mutable.ArrayBuffer[String] = ArrayBuffer[String]()
      if (Seq("rdbms", "jdbc").contains(config.getOrElse("readerFormat", ""))) {
        var rOptions: collection.mutable.Map[String, String] = collection.mutable.Map() ++ config
        var errorTable: collection.mutable.Map[String, String] = collection.mutable.Map()
        var origTargetKey: String = ""
        if (rOptions.getOrElse("sourceTableList", "").nonEmpty) {
          listedTables ++= rOptions("sourceTableList").split(",")
        } else {
          listedTables += rOptions.getOrElse("sourceTable", "")
        }
        if (config.contains("Driver")) {
          rOptions.put("Driver", config("Driver"))
        } else {
          rOptions.put("Driver", getDriverClass(config.getOrElse("rdbmsUrl", "")))
        }

        if (rOptions.getOrElse("sqlFile", "").nonEmpty) {
          rOptions ++= loadIngestSQL(config)
          rOptions.remove("mode")
          dataframeWriter(config, dataframeReader(rOptions.toMap))
        } else {
          if (localConfig.contains("targetKey")) {
            origTargetKey = localConfig("targetKey")
          }
          TableExtract(listedTables, rOptions, localConfig, origTargetKey)
        }
      } else if (config.getOrElse("readerFormat", "").equalsIgnoreCase("salesforce")) {
        //extractSalesforceData(config)

      } else {
        throw new InputMismatchException(s"Unknown readerFormat: ${config.getOrElse("readerFormat", "")} currently supports only rdbms,jdbc,salesforce")
      }
    } catch {
      case e: Exception =>
        logger.error("Unable to Load Data from JDBC Source")
        throw e
    }
  }

  def TableExtract(tableList: collection.mutable.ArrayBuffer[String],
                   rOptions: collection.mutable.Map[String, String],
                   localConfig: collection.mutable.Map[String, String],
                   origTargetKey: String): Unit = {

    val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(rOptions.getOrElse("extractionParallelism", "1").toInt))
    var extractedTables: collection.mutable.ArrayBuffer[String] = ArrayBuffer[String]()
    var errorTable: collection.mutable.Map[String, String] = collection.mutable.Map()
    val parallel = tableList.par
    parallel.tasksupport = taskSupport
    parallel.map(table => {
      logger.info(s"Preparing to extract data from: ${table}")
      rOptions.put("sourceTable", table)
      rOptions.put("targetTable", table)
      rOptions.put("targetKey", table)
      localConfig.put("targetTable", table)
      if (origTargetKey.nonEmpty) {
        localConfig.put("targetKey", origTargetKey)
      } else {
        localConfig.put("targetKey", table)
      }
      try {
        rOptions ++= loadIngestSQL(rOptions.toMap)
        rOptions.remove("mode")
        dataframeWriter(localConfig.toMap, dataframeReader(rOptions.toMap))
        logger.info(s"Successfully extracted table: ${table}")
        extractedTables.append(table)
      } catch {
        case e: Exception => {
          errorTable.put(table, e.getMessage)
          logger.error(s"Unable to extract and copy data from table: ${table}")
        }
      }
    }
    )
    if (extractedTables.nonEmpty) {
      logger.error("*****************************************\n\n")
      logger.error("Extracted Table List\n\n")
      logger.error("*****************************************\n\n")
      extractedTables.foreach(x => logger.info(x))
    }
    if (errorTable.nonEmpty) {
      logger.error("*****************************************\n\n")
      logger.error("Unable extract data from following tables\n\n")
      logger.error("*****************************************\n\n")
      logger.error("Table Name".formatted(s"%40s") + " : " + "Reason".formatted(s"%110s"))
      collection.mutable.LinkedHashMap(errorTable.toSeq.sortBy(_._1): _*)
        .foreach { x =>
          logger.error(x._1.formatted(s"%40s") + " : " + x._2.formatted(s"%110s"))
        }
      throw new IncompleteExtraction("Not all the tables extracted!!")
    }
  }

}

object IngestData {
  def apply(config: Map[String, String]): Unit = (new IngestData).ingestData(config)
}
