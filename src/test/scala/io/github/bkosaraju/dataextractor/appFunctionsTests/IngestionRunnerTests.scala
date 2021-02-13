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

import io.github.bkosaraju.dataextractor.{AppInterface, IngestionRunner}
import org.h2.jdbcx.JdbcDataSource
import org.h2.tools.RunScript
import org.scalatest.FunSuite

import java.io.FileReader

trait IngestionRunnerTests extends FunSuite with AppInterface {

  val h2Props = "src/test/resources/H2/SparkTestConnection.properties".loadParms()

  val ds = new JdbcDataSource()
  ds.setURL("jdbc:h2:mem:sparkTests")
  ds.setPassword(h2Props.getProperty("password", ""))
  ds.setUser(h2Props.getProperty("user", "sa"))
  val connection = ds.getConnection
  //val connection = DriverManager.getConnection("jdbc:h2:mem:test;LOCK_MODE=3;TRACE_LEVEL_FILE=3",h2Props)
  RunScript.execute(connection, new FileReader("src/test/resources/H2/sparkTests.DDL"))
  RunScript.execute(connection, new FileReader("src/test/resources/H2/sparkTests.DML"))
  connection.commit()
  val res = connection.createStatement().executeQuery("select count(1) from sparkTests.employees")

  test("appTests : Test H2 database connectivity with Initialized table") {
    assertResult(9999) {
      var re: BigDecimal = 0
      while (res.next()) {
        re = res.getBigDecimal(1)
      }
      re
    }
  }

  private val externalArgs = Array("src/test/resources/jdbcDataSource/H2jdbc_sqlFile.properties", "")

  test("IngestionRunner : Load the data from RDBMS into to URI - SQL File based extract") {
    assertResult(true) {
      IngestionRunner.ingestionRunner(externalArgs)
      true
    }
  }

  private val externalArgsDBTbl = Array("src/test/resources/jdbcDataSource/H2jdbc_dbTbl.properties", "")
  test("IngestionRunner : Load the data from RDBMS into to URI - Full extract by specifying Databese and Table") {
    assertResult(9999) {
      IngestionRunner.ingestionRunner(externalArgsDBTbl)
      val resDF = sparkSession.read.orc("build/jdbc/test_dbTbl")
      resDF.show(100, false)
      resDF.count()
    }
  }

  private val externalArgsFilter = Array("src/test/resources/jdbcDataSource/H2jdbc_sqlFileFilter.properties", "hire_date=1988-11-11")
  test("IngestionRunner : Load the data from RDBMS into to URI - Full extract by sql File Template using filtered") {
    assertResult(4) {
      IngestionRunner.ingestionRunner(externalArgsFilter)
      val resDF = sparkSession.read.orc("build/jdbc/test_filteredExtract")
      resDF.show(100, false)
      resDF.count()
    }
  }

  private val externalArgsEncryption = Array("src/test/resources/jdbcDataSource/H2jdbc_dbTbl_encryption.properties")
  test("IngestionRunner : Load the data from RDBMS into to URI - Using Data Encryption") {
    assertResult(9999) {
      IngestionRunner.ingestionRunner(externalArgsEncryption)
      val resDF = sparkSession.read.orc("build/jdbc/test_dbTbl_encryption")
      println("Encrypted Dataframe:")
      resDF.show(100, false)
      println("Encrypted filter data:")
      resDF.where("""EMP_NO in (18931,13932,10002,10003)""").show(false)
      resDF.count()
    }
  }

  private val exception = Array("src/test/resources/jdbcDataSource/H2jdbc_sqlFileFilter.properties")
  test("IngestionRunner : Raise an exception in case if required arguments were not passed ") {
    intercept[Exception] {
      IngestionRunner.ingestionRunner(exception)
    }
  }

  private val externalArgsFilterException = Array("src/test/resources/jdbcDataSource/H2jdbc_sqlFileFilter.properties", "")
  test("IngestionRunner : Raise an exception in case if required arguments were not passed  in case if there is any issue while processing") {
    intercept[Exception] {
      IngestionRunner.ingestionRunner(externalArgsFilterException)
      val resDF = sparkSession.read.orc("build/jdbc/test_filteredExtract")
      resDF.show(100, false)
      resDF.count()
    }
  }

  test("ingestData : Raise an exception in case if required arugments not passed") {
    intercept[Exception] {
      IngestionRunner.ingestionRunner(Array())
    }
  }

}
