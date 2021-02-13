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

import io.github.bkosaraju.utils.common.{CryptoSuite, LoadProperties}
import org.apache.commons.io.FileUtils

import java.io.{File, FileInputStream}
import java.nio.file.Files
import java.util.InputMismatchException
import scala.collection.JavaConverters._
import scala.util.Try

object IngestionRunner extends AppFunctions with LoadProperties with CryptoSuite {
  var content: String = _
  val templateFileLocation = Files.createTempDirectory("")

  def ingestionRunner(args: Array[String]): Unit = {
    if (args.length < 1) {
      logger.error(
        "Required Arguments Not Provided as properties file needed for running application")
      throw new InputMismatchException("Required Arguments Not Provided as properties file needed for runnig application")
    }
    val props = args(0).loadParms()
    val mode = props.getProperty("mode")
    val appName = props.getProperty("appName")
    try {
      if (args.length > 1) {
        props.setProperty("dwsVars", args(1).toString)
      }
      if (props.containsKey("encryptedConfig")) {
        if (props.containsKey("encryptedConfigKey")) {
          decryptFile(props.getProperty("encryptedConfig"), templateFileLocation + "/encData", props.getProperty("encryptedConfigKey"))
          props.load(new FileInputStream(templateFileLocation + "/encData"))
        } else {
          logger.warn("cant get encryptionKey from configuration hence ignoring encrypted config[may case failure laterpoint of time]")
        }
      }
      IngestData(props.asInstanceOf[java.util.Map[String, String]].asScala
        .map(x => x._1 -> x._2.replaceAll(""""""", "")).toMap
      )
    } catch {
      case e: Exception => {
        logger.error("Exception Occurred While Processing Data", e)
        if (props.getProperty("sendMailFlag", "").equalsIgnoreCase("true")) {
          sendMailFromError(e, props.asInstanceOf[java.util.Map[String, String]].asScala.toMap)
        }
        Try(FileUtils.deleteDirectory(new File(templateFileLocation.toString)))
        throw e
      }
    }
  }
}