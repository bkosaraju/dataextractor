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

object Launcher extends AppFunctions {
  def main(args: Array[String]): Unit = {
    try {
      IngestionRunner.ingestionRunner(args)
      if (sparkSession.conf.getAll.getOrElse("spark.master", "").contains("k8s:")) {
        sparkSession.stop()
      }
    } catch {
      case e: Exception => {
        logger.error("Error Occurred while processing job..")
        if (sparkSession.conf.getAll.getOrElse("spark.master", "").contains("k8s:")) {
          sparkSession.stop()
        }
        throw e
      }
    }
  }
}