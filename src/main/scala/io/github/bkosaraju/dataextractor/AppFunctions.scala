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

import functions._
import io.github.bkosaraju.utils.common.LoadProperties
import io.github.bkosaraju.utils.database.GetDriverClass
import io.github.bkosaraju.utils.spark.{DataFrameWriter, DataframeReader}


trait AppFunctions
  extends LoadIngestSQL
//    with GenSFSOQL
//    with ObjectDefs
    with StandardizeObjects
//    with SalesforceExtractor
    with LoadProperties
    with DataFrameWriter
    with GetDriverClass
    with DataframeReader
    with Exceptions
