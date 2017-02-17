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
  val resolutionRepos = Seq(
    "Akka Repository" at "http://repo.akka.io/releases/",
    "Spray Repository" at "http://repo.spray.cc/",
    "Scalaz Bintray Repo" at "https://dl.bintray.com/scalaz/releases"

  )

  object V {
    // Java
    val gcpDataflow          = "1.9.0"
    val gcpBigtable          = "0.9.4"
    val hadoop               = "2.7.3"
    val hbase                = "1.2.4"
    val typesafe             = "1.3.1"
    val jodaConvert          = "1.8.1"
    val jodaTime             = "2.9.7"
    val alpn                 = "8.1.5.v20150921"
    val boringssl            = "1.1.33.Fork26"
    //val tomcat               = "8.0.8"
	val slf4j				 = "1.7.22"

    // Scala
    val argot                = "1.0.4"
    val specs2               = "3.3"
    val guava                = "19.0"
    val json4s               = "3.2.11"
  }

  object Libraries {
    // Java
    val gcpBigtableHBase      = "com.google.cloud.bigtable"  % "bigtable-hbase-dataflow"            % V.gcpBigtable
    val hadoopCommon          = "org.apache.hadoop"          % "hadoop-common"                      % V.hadoop
    val hbaseCommon           = "org.apache.hbase"           % "hbase-common"                       % V.hbase
    val hbaseClient           = "org.apache.hbase"           % "hbase-client"                       % V.hbase
    val typesafe              = "com.typesafe"               % "config"                             % V.typesafe
    val alpn                  = "org.mortbay.jetty.alpn"     % "alpn-boot"                          % V.alpn
    //val tomcat                = "org.apache.tomcat"          % "tomcat-jni"                         % V.tomcat
    val boringssl             = "io.netty"                   % "netty-tcnative-boringssl-static"    % V.boringssl
    val jodaConvert           = "org.joda"                   % "joda-convert"                       % V.jodaConvert
    val jodaTime              = "joda-time"                   % "joda-time"                          % V.jodaTime
	val slf4j				  = "org.slf4j"					 % "slf4j-api"  						% V.slf4j

    // Scala
    val argot                 = "org.clapper"                %% "argot"                             % V.argot
    val json4s                = "org.json4s"                 %% "json4s-jackson"                    % V.json4s

    // Scala (test only)
    val specs2                = "org.specs2"       % "specs2_2.10"                  % V.specs2       % "test"
    val guava                 = "com.google.guava" % "guava"                        % V.guava        % "test"

    // Add additional libraries from mvnrepository.com (SBT syntax) here...
  }
}
