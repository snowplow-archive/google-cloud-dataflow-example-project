/*
 * Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.dataflow.streaming
package storage

// Java
import java.util.Date
import java.util.TimeZone
import java.text.SimpleDateFormat

//Bigtable
import com.google.cloud.bigtable.hbase.BigtableConfiguration;

//HBase
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;



/**
 * Object sets up singleton that finds AWS credentials for DynamoDB to access the
 * aggregation records table. The utility function below puts items into the
 * "AggregateRecords" table.
 */
object BigtableUtils {

  val dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  val timezone = TimeZone.getTimeZone("UTC")

  /**
   * Function timezone helper
   */
  def timeNow(): String = {
    dateFormatter.setTimeZone(timezone)
    dateFormatter.format(new Date())
  }


  /**
   * Function wraps admin setup
   */
  def setupBigtableAdmin(projectId: String, instanceId): Connection = {
    val connection = BigtableConfiguration.connect(projectId, instanceId)
    val admin = connection.getAdmin
    connection
  }


  /**
   * Function wraps get or create item in Bigtable table
   */
  def setOrUpdateCount(bigtableAdmin: Admin, tableName: String, bucketStart: String, eventType: String, createdAt: String,  updatedAt: String, count: Int){

    val rowInTable = getItem(bigtableAdmin: Admin, tableName, bucketStart, eventType)
    println(rowInTable)
    if (rowInTable == null) {
      BigtableUtils.putItem(bigtableAdmin: Admin, tableName, bucketStart, eventType, createdAt, updatedAt, count)
    } else {
      val oldCreatedAt = String(rowInTable.getValue("cf1".getBytes, "CreatedAt".getBytes))
      val oldCount = String(rowInTable.getValue("cf1".getBytes, "Count")).toInt
      val newCount = oldCount + count.toInt
      BigtableUtils.putItem(bigtableAdmin: Admin, tableName, bucketStart, eventType, oldCreatedAt, updatedAt, newCount)
    }
  }


  /**
   * Function wraps get row operation from Bigtable
   */
  def getItem(bigtableAdmin: Admin, tableName: String, bucketStart: String, eventType: String): Result = {

    val table = bigtableAdmin.getConnection().getTable(TableName.valueOf(tableName))
    Result getResult = table.get(new Get((bucketStart+":"+eventType).getBytes)
    getResult
  }


  /**
   * Function wraps Bigtable putItem operation
   */
  def putItem(bigtableAdmin: Admin, tableName: String, bucketStart: String, eventType: String, createdAt: String,  updatedAt: String, count: Int) {

    // Row column names
    val tableEventTypeSecondaryKeyName = "EventType"
    val tableCreatedAtColumnName = "CreatedAt"
    val tableUpdatedAtColumnName = "UpdatedAt"
    val tableCountColumnName = "Count"

    try {
      val time = new Date().getTime - (1 * 24 * 60 * 60 * 1000)
      val date = new Date()
      date.setTime(time)
      dateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"))
      val table = bigtableAdmin.getConnection().getTable(TableName.valueOf(tableName))
      println("Adding data to " + tableName)


      // key composed of "bucketStart:eventType"
      val put = new Put((bucketStart+":"+eventType).getBytes)
        .addColumn("cf1".getBytes, tableEventTypeSecondaryKeyName, eventType.getBytes)
        .addColumn("cf1".getBytes, tableCreatedAtColumnName, createdAt.getBytes)
        .addColumn("cf1".getBytes, tableUpdatedAtColumnName, updatedAt.getBytes)
        .addColumn("cf1".getBytes, tableCountColumnName, count.getBytes)

      // saving the data to Bigtable
      // println(item)
      table.put(put)
    } catch {
      case e: Exception => {
        System.err.println("Failed to create item in " + tableName)
        System.err.println(e.getMessage)
      }
    }
  }
}
