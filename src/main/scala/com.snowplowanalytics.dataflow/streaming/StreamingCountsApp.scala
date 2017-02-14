/*
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
import java.io.File
import java.io.FileReader
import java.util.Properties

// Config
import com.typesafe.config.{Config, ConfigFactory}

// Argot
import org.clapper.argot._


/**
 * The entry point class for the Dataflow Streaming Application.
 *
*/
object StreamingCountsApp {

  def main(args: Array[String]) {

    // General bumf for our app
    val parser = new ArgotParser(
      programName = "generated",
      compactUsage = true,
      preUsage = Some("%s: Version %s. Copyright (c) 2017, %s.".format(
        generated.Settings.name,
        generated.Settings.version,
        generated.Settings.organization)
      )
    )

    // Optional config argument
    val config = parser.option[Config](List("config"),
      "filename",
      "Configuration file.") {
      (c, opt) =>

        val file = new File(c)
        if (file.exists) {
          ConfigFactory.parseFile(file)
        } else {
          parser.usage("Configuration file \"%s\" does not exist".format(c))
          ConfigFactory.empty()
        }
    }
    parser.parse(args)

    // read the config file if --config parameter is provided else fail
    val conf = config.value.getOrElse(throw new RuntimeException("--config argument must be provided"))

    // create Spark Streaming Config from hocon file in resource directory
    val scc = StreamingCountsConfig(
      tableName       = conf.getConfig("bigtable").getString("tableName"),
      projectId       = conf.getConfig("googlecloud").getString("projectId"),
      instanceId      = conf.getConfig("bigtable").getString("instanceId"),
      stagingLocation = conf.getConfig("dataflow").getString("stagingLocation"),
      topicName       = conf.getConfig("cloudpubsub").getString("topic")
    )

    // start StreamingCounts application with config object
    StreamingCounts.execute(scc)
  }
}
