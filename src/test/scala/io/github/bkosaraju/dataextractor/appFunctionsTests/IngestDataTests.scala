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
package io.github.bkosaraju.dataextractor.appFunctionsTests

import io.github.bkosaraju.dataextractor.{AppInterface, IngestData}
import org.apache.commons.io.FileUtils

import java.io.File
import scala.collection.JavaConverters._

trait IngestDataTests extends AppInterface {

  private val config = "src/test/resources/jdbcDataSource/H2jdbc_dbTbl.properties".loadParms().asInstanceOf[java.util.Map[String, String]].asScala
  test("ingestData : Raise an exception in case if there is any issue while Ingesting the data to Platform - exception while reading parameters") {
    intercept[Exception] {
      config.remove("rdbmsUrl")
      IngestData(config.toMap)
    }
  }
  private val jdbcConfig = "src/test/resources/jdbcDataSource/H2jdbc_dbTbl.properties".loadParms().asInstanceOf[java.util.Map[String, String]].asScala
  test("ingestData : Load data followed by root file system under the keyspace") {
    assertResult(9999) {
      jdbcConfig.remove("targetURI")
      jdbcConfig.put("targetKey", "tmp/test_dbTbl")
      IngestData(jdbcConfig.toMap)
      val resultRows = context.read.orc("/tmp/test_dbTbl").count()
      FileUtils.deleteDirectory(new File("/tmp/test_dbTbl"))
      resultRows
    }
  }

  test("ingestData : Raise an exception in case if there is any issue while Ingesting the data to Platform - exception while reading data") {
    intercept[Exception] {
      config.put("sourceDatabase", "unKnownDatabase")
      config.put("targetKey", "test_dbTblException")
      IngestData(config.toMap)
    }
  }

}
