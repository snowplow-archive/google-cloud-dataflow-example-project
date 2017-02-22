/**
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
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
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTimeZone

//HBase
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Admin
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result

/**
 * Object handles communication with Bigtable. Connection, storage and retrieval
 */
object BigtableUtils {

  private val dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZoneUTC()

  /**
   * Function timezone helper
   */
  def timeNow(): String = dateFormatter.print(DateTime.now(DateTimeZone.UTC))

  /**
   * Function wraps store or update row operation from Bigtable
   * 
   * @param bigtableConnection The instance of a Connection to our Bigtable table
   * @param tableName Name of the table where the event counts will be stored
   * @param columnFamily Column family where the event counts will be stored
   * @param bucketStart Bucket where the event count will be stored (results of a downsampling of the event's timestamp)
   * @param eventType Type of the event whose count we'll update or set
   * @param createdAt String containing row creation timestamp
   * @param updatedAt String containing last row update timestamp
   * @param count Event count to set or increment
   */
  def setOrUpdateCount(bigtableConnection: Connection, tableName: String, columnFamily: String, 
    bucketStart: String, eventType: String, createdAt: String,  updatedAt: String, count: Int): Unit = {

    val rowInTable = getItem(bigtableConnection, tableName, bucketStart, eventType)
    if (rowInTable.isEmpty) {
      BigtableUtils.putItem(bigtableConnection, tableName, columnFamily, bucketStart, eventType, 
        createdAt, updatedAt, count)
    } else {
      val oldCreatedAt = new String(rowInTable.getValue(columnFamily.getBytes, "CreatedAt".getBytes))
      val oldCount = new String(rowInTable.getValue(columnFamily.getBytes, "Count".getBytes)).toInt
      val newCount = oldCount + count.toInt
      BigtableUtils.putItem(bigtableConnection, tableName, columnFamily, bucketStart, eventType, 
        oldCreatedAt, updatedAt, newCount)
    }
  }

  /**
   * Function wraps get row operation from Bigtable
   * 
   * @param bigtableConnection The instance of a Connection to our Bigtable table
   * @param tableName Name of the table where the event count is stored
   * @param bucketStart Bucket where the event count was stored (results of a downsampling of the event's timestamp)
   * @param bigtableConnection Type of the event to get
   * @return Result instance, containing the table row that corresponds to 
   * the specified event type, on the specified bucket. (Or an empty row, if no corresponding events were received)
   */
  def getItem(bigtableConnection: Connection, tableName: String, bucketStart: String, eventType: String): Result = {
    val table = bigtableConnection.getTable(TableName.valueOf(tableName))
    val getResult = table.get(new Get(s"$bucketStart:$eventType".getBytes))
    getResult
  }

  /**
   * Function wraps put row operation from Bigtable
   * 
   * @param bigtableConnection The instance of a Connection to our Bigtable table
   * @param tableName Name of the table where the event count will be stored
   * @param bucketStart Bucket where the event count will be stored (results of a downsampling of the event's timestamp)
   * @param bigtableConnection Type of the event to store
   */
  def putItem(bigtableConnection: Connection, tableName: String, columnFamily: String, 
    bucketStart: String, eventType: String, createdAt: String,  updatedAt: String, count: Int): Unit = {

    // Row column names
    val tableEventTypeSecondaryKeyName = "EventType".getBytes
    val tableCreatedAtColumnName = "CreatedAt".getBytes
    val tableUpdatedAtColumnName = "UpdatedAt".getBytes
    val tableCountColumnName = "Count".getBytes

    val table = bigtableConnection.getTable(TableName.valueOf(tableName))

    // key composed of "bucketStart:eventType"
    val put = new Put(s"$bucketStart:$eventType".getBytes)
      .addColumn(columnFamily.getBytes, tableEventTypeSecondaryKeyName, eventType.getBytes)
      .addColumn(columnFamily.getBytes, tableCreatedAtColumnName, createdAt.getBytes)
      .addColumn(columnFamily.getBytes, tableUpdatedAtColumnName, updatedAt.getBytes)
      .addColumn(columnFamily.getBytes, tableCountColumnName, (""+count).getBytes)

    // saving the data to Bigtable
    table.put(put)
  }
}
