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

// Config
import com.typesafe.config.ConfigFactory

/**
 * The entry point class for the Dataflow Streaming Application.
 */
object StreamingCountsApp {

  def main(args: Array[String]) {

    // Read the config file if -Dconfig.file is passed
    val conf = ConfigFactory.load()

    // Create a Google Cloud Dataflow Streaming Config from hocon file in resource directory
    val scc = StreamingCountsConfig(
      tableName       = conf.getString("bigtable.tableName"),
      columnFamily    = conf.getString("bigtable.columnFamily"),
      instanceId      = conf.getString("bigtable.instanceId"),
      projectId       = conf.getString("googlecloud.projectId"),
      stagingLocation = conf.getString("dataflow.stagingLocation"),
      topicName       = conf.getString("cloudpubsub.topic")
    )

    // Start StreamingCounts application with config object
    StreamingCounts.execute(scc)
  }
}
