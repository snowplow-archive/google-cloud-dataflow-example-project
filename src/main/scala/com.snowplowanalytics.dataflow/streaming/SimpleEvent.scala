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

// Java
import java.text.SimpleDateFormat
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime

// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.ext.JodaTimeSerializers

//Scala
import scala.util.Try

/**
 * Companion object for creating a SimpleEvent
 * from incoming JSON
 */
object SimpleEvent {

  /**
   * Converts date string into Date object
   */
  implicit val formats = new DefaultFormats {
    override def dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
  } ++ JodaTimeSerializers.all

  /**
   * Converts String of JSON data into SimpleEvent objects
   */
  def fromJson(jsonString: String): Option[SimpleEvent] = Try(parse(jsonString).extract[SimpleEvent]).toOption
}

/**
 * Simple Class demonstrating an EventType log consisting of:
 *   1. ISO 8601 DateTime Object that will be downsampled
 *      (see BucketingStrategy.scala file for more details)
 *   2. A simple model of colors for this EventType:
 *      'Red','Orange','Yellow','Green', or 'Blue'
 *   example log: {"timestamp": "2017-06-05T13:00:22.540374", "type": "Orange", "id": "018dd633-f4c3-4599-9b44-ebf71a1c519f"}
 */
case class SimpleEvent(id: String, timestamp: DateTime, `type`: String) {
  //Bucket time format
  private val BucketToMinuteFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:00.000")

  // Convert timestamp into Time Bucket
  val bucket: String = BucketToMinuteFormatter.print(timestamp)
}
