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
//package io.github.bkosaraju.dataextractor.functions
//
//import com.springml.salesforce.wave.api.{APIFactory, ForceAPI}
//
//import scala.collection.JavaConverters._
//
//trait GenSFSOQL extends Session with Exceptions {
//
//
//  def getAPI(config: Map[String, String]): ForceAPI = {
//
//    if (config.contains("salesforceUser") && config.contains("salesforcePassword") && config.contains("salesforceUrl")) {
//      APIFactory.getInstance().forceAPI(
//        config("salesforceUser"),
//        config("salesforcePassword"),
//        config("salesforceUrl"),
//        config.getOrElse("salesforceApiVersion", "35.0")
//      )
//    } else {
//      logger.error("salesforceUser, salesforcePassword, salesforceUrl are mandatory parameters for retrieving salesforce data")
//      throw new InvalidInputParameters("salesforceUser, salesforcePassword, salesforceUrl are mandatory parameters for retrieving salesforce data")
//    }
//  }
//
//  def getSFObjects(config: Map[String, String]): List[String] = {
//    val sfAPI = getAPI(config)
//    sfAPI.sObjects().getSobjects.asScala.filter(_.getQueryable.equals(true)).map(_.getName).toList
//  }
//
//  def getObjectColumns(objectName: String, config: Map[String, String]): List[String] = {
//    val sfAPI = getAPI(config)
//    try {
//      sfAPI.describeSalesforceObject(objectName).getFields.asScala.map(_.getName).toList
//    } catch {
//      case e: Exception => {
//        logger.error(s"Unable to generate columns list for ${objectName}")
//        List("1")
//      }
//    }
//  }
//
//  @deprecated(" please use getSfSoql with filter clause", "")
//  def genSfSoql(objectName: String, colList: List[String]): String = {
//    val soql = s"""SELECT ${colList.mkString(",")} FROM ${objectName}"""
//    logger.info(s"soql for ${objectName} --> ${soql}")
//    soql
//  }
//
//  def genSfSoql(objectName: String, colList: List[String], filterClause: String): String = {
//    val soql = s"""SELECT ${colList.mkString(",")} FROM ${objectName} ${filterClause}"""
//    logger.info(s"soql for ${objectName} --> ${soql}")
//    soql
//  }
//
//
//  def getFilterClause(config: Map[String, String]): String = {
//    if (config.contains("extractFilter")) {
//      var filterClause = config("extractFilter")
//      config.keys.foreach(r => filterClause = filterClause.replaceAll(s"#${r}#", config(r)))
//      " where " + filterClause
//    } else ""
//  }
//
//  def genAllSOQL(config: Map[String, String]): Map[String, String] = {
//    val soqlMap: collection.mutable.Map[String, String] = collection.mutable.Map()
//    val filter = getFilterClause(config)
//    for (sfObj <- getSFObjects(config)) yield soqlMap.put(sfObj, genSfSoql(sfObj, getObjectColumns(sfObj, config), filter))
//    soqlMap.toMap
//  }
//
//  def genListedSOQL(objectList: List[String], config: Map[String, String]): Map[String, String] = {
//    val soqlMap: collection.mutable.Map[String, String] = collection.mutable.Map()
//    val allObjects = getSFObjects(config).map(_.toLowerCase)
//    for (sfObj <- objectList) {
//      if (allObjects.contains(sfObj.toLowerCase)) {
//        logger.info(s"found object ${sfObj} in Sales force proceeding for query extraction..")
//        soqlMap.put(sfObj, genSfSoql(sfObj, getObjectColumns(sfObj, config), getFilterClause(config)))
//      } else {
//        soqlMap.put(sfObj, "MISSING_FROM_SOURCE")
//        logger.warn(s"Unable to find given input object ${sfObj} in salesforce.. ")
//      }
//    }
//    soqlMap.toMap
//  }
//
//  def getSOQLs(config: Map[String, String]): Map[String, String] = {
//    if (config.getOrElse("salesforceObjects", "").eq("")) {
//      genAllSOQL(config)
//    } else {
//      genListedSOQL(config("salesforceObjects").split(",").toList, config)
//    }
//  }
//
//}
