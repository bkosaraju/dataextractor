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

import io.github.bkosaraju.utils.common.SQLTemplateWrapper

trait LoadIngestSQL extends Session with SQLTemplateWrapper {
  /**
   * Method to generate Ingestion SQL from tempalte or database table name properties
   *
   * @note
   * sqlFile hase more precedence than sourceDatabase and sourceTable, in case if needed to be extracted with full table better to go with Database and Table names
   * @param config Map[String,String]
   * @return Map(Connection Properties)
   * @example loadIngestSQL(props)
   * {{{
   *                   if (config("sqlFile", "").ne("")) {
   *           (genIngestSQL(config).trim+";")
   *             .replaceAll("^", " ( ")
   *             .replaceAll(";+$", " ) sbTbl")
   *         } else {
   *           logger.info("No SQL file provided, continuing with full table extract !!")
   *           val srcDB = config("sourceDatabase")
   *           val srcTbl = config("sourceTable")
   *           srcDB + "." + srcTbl
   *         }
   *       }
   *         Map("dbtable" -> sqlText,
   *           "url" -> config("rdbmsUrl"),
   *           "user" -> config("rdbmsUser"),
   *           "password" -> config("rdbmsPassword"))
   * }}}
   */
  def loadIngestSQL(config: Map[String, String]): Map[String, String] = {
    val resConf: collection.mutable.Map[String, String] = collection.mutable.Map()
    try {
      if (config("readerFormat").equalsIgnoreCase("salesforce")) {
        resConf.put("login", config("salesforceUrl"))
        resConf.put("username", config("salesforceUser"))
        resConf.put("password", config("salesforcePassword"))
        resConf.put("version", config.getOrElse("salesforceApiVersion", "35.0"))
        resConf.put("soql", getSQLFromTemplate(config).trim)
      }
      resConf.toMap
    } catch {
      case e: Throwable => {
        logger.error("Unable to Generate sql for Given input properties..", e)
        throw e
      }
    }
  }
}