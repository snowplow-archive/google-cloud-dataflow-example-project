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

import sbt._

object Dependencies {
  val resolutionRepos = Seq(
    "Akka Repository" at "http://repo.akka.io/releases/",
    "Spray Repository" at "http://repo.spray.cc/"
  )

  object V {
    // Java
    val googleCloud          = "0.8.1-alpha"
    val gcpPubSub            = "0.8.1-alpha"
    val gcpDataflow          = "1.9.0"
    val gcpBigtable          = "0.9.4"
    val hbase                = "1.3.0"
    val hadoop               = "1.2.1"
    // Scala
    val argot                = "1.0.3"
    // Add versions for your additional libraries here...
    // Scala (test)
    val specs2               = "1.13"
    val guava                = "11.0.1"
    val json4s               = "3.2.10"

  }

  object Libraries {
    // Java
    val googleCloud           = "com.google.cloud"          %% "google-cloud"                       % V.googleCloud
    val gcpPubSub             = "com.google.cloud"          %% "google-cloud-pubsub"                % V.googleCloud
    val gcpDataflow           = "com.google.cloud.dataflow" %% "google-cloud-dataflow-java-sdk-all" % V.gcpDataflow
    val gcpBigtableHBase      = "com.google.cloud.bigtable" %% "bigtable-hbase-1.2"                 % V.gcpBigtable
    val gcpBigtableDataflow   = "com.google.cloud.bigtable" %% "bigtable-hbase-dataflow"            % V.gcpBigtable
    //TODO: check if we really need all 3 of these hbase libraries
    val hbase                 = "org.apache.hbase"          %% "hbase"                              % V.hbase
    val hbaseCommon           = "org.apache.hbase"          %% "hbase-common"                       % V.hbase
    val hbaseClient           = "org.apache.hbase"          %% "hbase-client"                       % V.hbase
    val hadoop                = "org.apache.hadoop"         %% "hadoop-core"                        % V.hadoop

    // Scala
    val argot                 = "org.clapper"               %% "argot"                              % V.argot
    val json4s                = "org.json4s"                %% "json4s-jackson"                     % V.json4s

    // Scala (test only)
    val specs2                = "org.specs2"       % "specs2_2.10"                  % V.specs2       % "test"
    val guava                 = "com.google.guava" % "guava"                        % V.guava        % "test"

    // Add additional libraries from mvnrepository.com (SBT syntax) here...
  }
}
