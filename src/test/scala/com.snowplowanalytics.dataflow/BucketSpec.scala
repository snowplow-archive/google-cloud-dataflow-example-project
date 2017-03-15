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

//Scala
import org.specs2._
import scala.collection.JavaConverters._

//Dataflow testing
import com.google.cloud.dataflow.sdk.transforms.DoFnTester

class BucketSpec extends Specification { def is = s2"""

    Spec to test event bucketing. Using two json strings:
    
    Proper event-describing JSON string:
    '{  
        "timestamp": "2015-06-05T12:54:43.064528",
        "type": "Green",
        "id": "4ec80fb1-0963-4e35-8f54-ce760499d974"
    }'

    Faulty JSON string: 
    '{  
        "timestamp": "not-a-valid-timestamp",
        "type": "Green",
        "id": "4ec80fb1-0963-4e35-8f54-ce760499d974"
    }'   

    The faulty JSON string should produce no output, so the
    output list should:
      have length 1 $e1

    The correct JSON string should yield a tuple with
      a `type` field equal to 'Green' $e2
      a timestamp field equal to '2015-06-05T12:55:00.000' $e3
                                                 """

  // bucketEvents returns the DoFn that we want to test
  val bucketEventsTester = DoFnTester.of(StreamingCounts.bucketEvents)
  
  /**
   * Two sample inputs: 
   * - The first one should produce a correct KV object containing
   * the event's bucket and type: KV[String, String] -> [e.bucket, e.type]
   * - The second one is invalid and should produce no output
   */
  
  val json_event = "{\"timestamp\": \"2015-06-05T12:54:43.064528\",\"type\": \"Green\",\"id\": \"4ec80fb1-0963-4e35-8f54-ce760499d974\"}"
  val json_event2 = "{\"timestamp\": \"not-a-valid-timestamp\",\"type\": \"Green\",\"id\": \"4ec80fb1-0963-4e35-8f54-ce760499d974\"}"
  
  //list of test results
  val testOutputs = bucketEventsTester.processBatch(json_event, json_event2)
  
  def e1 = testOutputs.size must beEqualTo(1)
  def e2 = testOutputs.get(0).getValue must beEqualTo("Green")
  def e3 = testOutputs.get(0).getKey must beEqualTo("2015-06-05T12:55:00.000")

}
