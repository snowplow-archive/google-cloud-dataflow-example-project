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

import sbt._

object Dependencies {
  val resolutionRepos = Seq("Akka Repository" at "http://repo.akka.io/releases/")

  object V {
    // Java
    val gcpDataflow          = "1.9.0"
    val gcpBigtable          = "0.9.4"
    val hbase                = "1.2.4"
    val hadoop               = "2.7.3"
    val typesafe             = "1.3.1"
    val jodaTime             = "2.9.7"

    // Scala
    val specs2               = "3.7"
    val json4s               = "3.2.11"
  }

  object Libraries {
    // Java
    val gcpBigtableHBase      = "com.google.cloud.bigtable"  % "bigtable-hbase-dataflow"            % V.gcpBigtable
    val hbaseCommon           = "org.apache.hbase"           % "hbase-common"                       % V.hbase
    val hadoopCommon          = "org.apache.hadoop"          % "hadoop-common"                      % V.hadoop
    val hbaseClient           = "org.apache.hbase"           % "hbase-client"                       % V.hbase
    val typesafe              = "com.typesafe"               % "config"                             % V.typesafe
    val jodaTime              = "joda-time"                  % "joda-time"                          % V.jodaTime

    // Scala
    val json4s                = "org.json4s"                 %% "json4s-jackson"                    % V.json4s
    val json4sExt             = "org.json4s"                 %% "json4s-ext"                        % V.json4s

    // Scala (test only)
    val specs2                = "org.specs2"       % "specs2_2.11"                  % V.specs2       % "test"
  }
}
