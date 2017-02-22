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

import org.specs2._

class SimpleSpec extends Specification { def is = s2"""

    Spec to test JSON parsing
    
    '{  
        "timestamp": "2015-06-05T12:54:43.064528",
        "type": "Green",
        "id": "4ec80fb1-0963-4e35-8f54-ce760499d974"
    }'
    
    The previous JSON string should  
      yield a valid SimpleEvent $e1
      have a `type` field equal to 'Green' $e2

---------------------------------------------------------

    '{  
        "timestamp": "not-a-valid-timestamp",
        "type": "Green",
        "id": "4ec80fb1-0963-4e35-8f54-ce760499d974"
    }'

    The previous JSON string should  
      yield isDefined equal to true $e3
      be equal to None $e4
                                                 """

    val json_event = "{\"timestamp\": \"2015-06-05T12:54:43.064528\",\"type\": \"Green\",\"id\": \"4ec80fb1-0963-4e35-8f54-ce760499d974\"}"
    val se = SimpleEvent.fromJson(json_event)
    def e1 = se.isDefined must beEqualTo(true)
    def e2 = se.get.`type` must beEqualTo("Green")

    val json_event2 = "{\"timestamp\": \"not-a-valid-timestamp\",\"type\": \"Green\",\"id\": \"4ec80fb1-0963-4e35-8f54-ce760499d974\"}"
    val se2 = SimpleEvent.fromJson(json_event2)
    def e3 = se2.isDefined must beEqualTo(false)
    def e4 = se2 must beEqualTo(None)
}
