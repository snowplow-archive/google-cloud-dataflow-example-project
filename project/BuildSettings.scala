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
import Keys._

object BuildSettings {

  // Basic settings for our app
  lazy val basicSettings = Seq[Setting[_]](
    organization  := "com.snowplowanalytics",
    version       := "0.1.0-rc2",
    description   := "A Google Cloud Dataflow streaming job reading events from Cloud Pub/Sub and writing event counts to Bigtable",
    scalaVersion  := "2.11.8",
    resolvers     ++= Dependencies.resolutionRepos,

    fork in run := true
  )

  // sbt-assembly settings for building a fat jar
  import sbtassembly.AssemblyPlugin.autoImport._
  lazy val sbtAssemblySettings = Seq(

    // Simpler jar name
    assemblyJarName in assembly := {
      name.value + "-" + version.value + ".jar"
    },

    // Drop these jars
    assemblyExcludedJars in assembly := { 
      val cp = (fullClasspath in assembly).value
      val excludes = Set( 
        "commons-beanutils-core-1.8.0.jar", // Clash with each other and with commons-collections
        "commons-beanutils-1.7.0.jar",      // (Same as above)
        "bigtable-protos-0.3.0.jar",        // Conflict grpc-core-proto-0.0.3.jar
        "datastore-v1-protos-1.0.1.jar",    // (Same as above)
        "netty-all-4.0.23.Final.jar",       // Conflict with several netty jars imported independently
        "appengine-api-1.0-sdk-1.9.34.jar",
        "guava-jdk5-17.0.jar"
      )
      cp filter { jar => excludes(jar.data.getName) }
    },

    assemblyMergeStrategy in assembly := {
      case x if x.startsWith("META-INF") => MergeStrategy.discard // More bumf
      case x if x.endsWith(".html") => MergeStrategy.discard

      case x if x.startsWith("com/google/cloud/bigtable/") => MergeStrategy.last
      case x if x.startsWith("com/google/longrunning/") => MergeStrategy.last
      case x if x.startsWith("com/google/rpc/") => MergeStrategy.last
      case x if x.startsWith("com/google/type/") => MergeStrategy.last
      case x if x.startsWith("google/protobuf/") => MergeStrategy.last
     
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )

  lazy val buildSettings = basicSettings ++ sbtAssemblySettings
}
