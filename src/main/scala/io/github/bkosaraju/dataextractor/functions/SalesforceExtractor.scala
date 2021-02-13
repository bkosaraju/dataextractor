///*
// * Copyright (C) 2019-2021 bkosaraju
// * All Rights Reserved.
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *   http://www.apache.org/licenses/LICENSE-2.0
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// *
// */
//package io.github.bkosaraju.dataextractor.functions
//
//import io.github.bkosaraju.utils.common.SendMail
//import io.github.bkosaraju.utils.spark.{DataFrameWriter, DataframeReader}
//import org.apache.commons.io.FileUtils
//import org.apache.spark.sql.DataFrame
//
//import java.io.File
//import java.time.LocalDateTime
//import java.time.format.DateTimeFormatter
//import scala.collection.mutable.ArrayBuffer
//import scala.util.Try
//
////extends LoadIngestSQL
////with LoadStdDF
////with WriteTargetDF
////with StandardizeObjects
////with ObjectDefs
////with SendStatusMail {
//trait SalesforceExtractor
//  extends LoadIngestSQL
//    with StandardizeObjects
//    with DataframeReader
//    with DataFrameWriter
//    with ObjectDefs
//    with GenSFSOQL
//    with SendMail {
//
//  def extractSalesforceData(config: Map[String, String]): Unit = {
//
//    val dftmr = DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss")
//    val mfData: ArrayBuffer[AuditElements] = ArrayBuffer[AuditElements]()
//    //Provided SQL
//    if (config.getOrElse("salesforceObjects", "").isEmpty && config.contains("sqlFile")) {
//      var rOptions: collection.mutable.Map[String, String] = collection.mutable.Map()
//      rOptions ++= loadIngestSQL(config)
//      rOptions ++= Map("readerFormat" -> "com.springml.spark.salesforce")
//      val tgtDF = dataframeReader(rOptions.toMap)
//      if (tgtDF.isEmpty) {
//        logger.warn(s"No data found for given soql : ${rOptions("soql")} hence skipping ...")
//      } else {
//        dataframeWriter(config, sanitizeNulls(tgtDF))
//      }
//    } else {
//      //All Objects
//      var sfOptions, erroredObjects: collection.mutable.Map[String, String] = collection.mutable.Map()
//      sfOptions.put("login", config("salesforceUrl"))
//      sfOptions.put("username", config("salesforceUser"))
//      sfOptions.put("password", config("salesforcePassword"))
//      sfOptions.put("version", config.getOrElse("salesforceApiVersion", "46.0"))
//      var sfDfs: collection.mutable.Map[String, DataFrame] = collection.mutable.Map[String, DataFrame]()
//      val selObjects = getSOQLs(config)
//      for (obj <- selObjects.filter(_._2 != "MISSING_FROM_SOURCE").keySet.par) {
//        logger.info(s"loading data for object ${obj}")
//        val srcExtractedDF = try {
//          dataframeReader(sfOptions.toMap ++ Map("soql" -> selObjects(obj)) ++ Map("readerFormat" -> "com.springml.spark.salesforce"))
//        } catch {
//          case e: Exception => {
//            logger.warn(s"Unable to retrieve object ${obj} using soql,  ${e.getMessage} skipping object")
//            erroredObjects.put(obj, e.getMessage)
//            sparkSession.emptyDataFrame
//          }
//        }
//
//
//        if (Try(srcExtractedDF.isEmpty).isSuccess) {
//          if (Try(srcExtractedDF.isEmpty).getOrElse(true)) {
//            if (erroredObjects.keySet.contains(obj)) {
//              mfData += AuditElements(
//                obj, 0, None, LocalDateTime.now.format(dftmr), erroredObjects(obj)
//                  .replaceAll("[,\n]", " ")
//              )
//            } else {
//              mfData += AuditElements(
//                obj, 0, Some(true), LocalDateTime.now.format(dftmr)
//              )
//              logger.warn(s"No data found for object ${obj} hence skipping ...")
//            }
//          } else {
//            mfData += AuditElements(
//              obj, Try(srcExtractedDF.count()).getOrElse(0), Some(false),
//              LocalDateTime.now.format(dftmr)
//            )
//            dataframeWriter(config ++ Map("sfTargetKey" -> obj), sanitizeNulls(srcExtractedDF))
//            logger.info(s"Successfully copied data for object: ${obj}")
//          }
//        } else {
//          logger.warn(s"Unable to retrieve data from source for object: ${obj} ")
//          mfData += AuditElements(
//            obj, 0, Some(false), LocalDateTime.now.format(dftmr)
//            , "Exception - Unable to retrieve data from source for object"
//          )
//        }
//      }
//      for (mObj <- selObjects.filter(_._2 == "MISSING_FROM_SOURCE").keySet) {
//        mfData += AuditElements(
//          mObj, 0, None, LocalDateTime.now.format(dftmr)
//          , "Missing / Non Queryable Object from Source"
//        )
//      }
//      //Printing Manifest
//      val mfd: ArrayBuffer[String] = ArrayBuffer(
//        """
//          |<!DOCTYPE html>
//          |<html>
//          |<head>
//          |<style>
//          |table {
//          |  font-family: calibri;
//          |  border-collapse: collapse;
//          |  width: 100%;
//          |  color: 585656;
//          |}
//          |
//          |td, th {
//          |  border: 1px solid #dddddd;
//          |  text-align: left;
//          |  padding: 8px;
//          |}
//          |
//          |tr:nth-child(even) {
//          |  background-color: #dddddd;
//          |}
//          |</style>
//          |</head>
//          |<body>
//          |<p>Hi There,</p>
//          |<p>Please find Salesforce extraction summary.&nbsp;</p>
//          |<p></p>
//          |<p></p>
//          |<h2>Application Audit summary:</h2>
//          |<table><tr><th>Object Name</th><th>Extracted Records</th><th>Is Empty</th><th>Extracted Timestamp</th><th>Comment</th></tr>""".stripMargin
//      )
//      var manifestData: ArrayBuffer[String] = ArrayBuffer("Object Name,Extracted Records,Is Empty, Extracted Timestamp,Comment")
//      for (adt <- mfData) {
//        val str = (
//          "<tr>\n<td>" +
//            adt.objectName + "</td>\n<td>" +
//            +adt.recordCount + "</td>\n<td>"
//            + adt.isEmpty.getOrElse("NA").toString + "</td>\n<td>"
//            + adt.extractedTimestamp + "</td>\n<td>"
//            + adt.comment + "</td></tr>\n"
//          )
//        mfd += str
//        manifestData += Seq(adt.objectName, adt.recordCount, adt.isEmpty.getOrElse("NA"), adt.extractedTimestamp, adt.comment.replaceAll("[,\n]", "")).mkString(",")
//      }
//      mfd +=
//        """
//          |</table>
//          |</body>
//          |</html>
//          |""".stripMargin
//
//      val mailSpecs = Map(
//        "subject" -> "Salesforce Object Extraction Summary",
//        "content" -> mfd.mkString("\n")
//      )
//      if (config.getOrElse("sendMailFlag", "").equalsIgnoreCase("true")) {
//        sendMail(config ++ mailSpecs)
//        Try(FileUtils.writeStringToFile(
//          new File(config("targetURI").replaceAll("file://", "") + "/SF_Extraction_Audit_" + LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMddHHmmss")) + ".csv")
//          , manifestData.mkString("\n"), "UTF-8"))
//          .getOrElse(
//            logger.warn("Unable to write Audit log data"))
//      }
//    }
//  }
//}
