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
package com.snowplowanalytics.dataflow.streaming

/**
 * Core of the Dataflow Streaming Application
 * 1. Configuration information is brought in from StreamingCountsApp.scala
 * 2. Object sets up Dataflow, PubSub and Bigtable
 * 3. Once connections are up, Dataflow StreamingCounts stream processing starts
 * Pub/Sub     -> Dataflow               -> Bigtable
 * Raw Data    -> Stream Processing Data -> Stored in Database
 *
 */


import java.lang.{Long => JLong, Iterable => JIterable}
import scala.collection.JavaConverters._

// Dataflow
import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.io.PubsubIO
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner
import com.google.cloud.dataflow.sdk.transforms.Count
import com.google.cloud.dataflow.sdk.transforms.DoFn
import com.google.cloud.dataflow.sdk.transforms.ParDo
import com.google.cloud.dataflow.sdk.transforms.GroupByKey
import com.google.cloud.dataflow.sdk.transforms.Create
import com.google.cloud.dataflow.sdk.transforms.Flatten
import com.google.cloud.dataflow.sdk.values.KV
import com.google.cloud.dataflow.sdk.values.PDone

//HBase
import org.apache.hadoop.hbase.client.Connection

// This project
import storage.BigtableUtils

trait PipelineOptionsWithBigtable extends DataflowPipelineOptions {
    def getConnection() : Connection
    def setConnection(value: Connection) : Unit
    def getTablename() : String
    def setTablename(value: String) : Unit
}

object StreamingCounts {

  /**
   * Private function to set up Dataflow
   *
   * @param config The configuration for our job using StreamingCountsConfig.scala
   */
  private def setupDataflow(config: StreamingCountsConfig): Pipeline = {
    val dataflowPipeline = {

        PipelineOptionsFactory.register(classOf[PipelineOptionsWithBigtable]) 

        //needed to access bigtable connection inside DoFn's
        val bigtableConnection = BigtableUtils
                .setupBigtable(config.projectId, config.instanceId)

        val options = PipelineOptionsFactory
                .as(classOf[PipelineOptionsWithBigtable])

        options.setRunner(classOf[BlockingDataflowPipelineRunner])
        options.setProject(config.projectId)
        options.setStagingLocation(config.stagingLocation)
        options.setConnection(bigtableConnection)

        // Create the Pipeline object with the options we defined above.
        val p = Pipeline.create(options)
        p
    }
    dataflowPipeline
  }

  /**
   * function applied in the "map" phase
   */
  def bucketEvents = new DoFn[String,KV[String,String]]() {
    @Override
    def processElement(c: DoFn[String,KV[String,String]]#ProcessContext) {
        val e = SimpleEvent.fromJson(c.element)
        c.output(KV.of(e.bucket, e.`type`))
    }
  }

  /**
   * function applied to count the events per type in each bucket and 
   * store the counts in Bigtable
   */
  def storeEventCounts = new DoFn[KV[String,JIterable[String]], PDone]() {
    @Override
    def processElement(c: DoFn[KV[String,JIterable[String]], PDone]#ProcessContext) {
        val bucketName = c.element.getKey
        val bigtableConnection = c
            .getPipelineOptions()
            .as(classOf[PipelineOptionsWithBigtable])
            .getConnection()
        val tableName = c
            .getPipelineOptions()
            .as(classOf[PipelineOptionsWithBigtable])
            .getTablename()

        val counts = c.element.getValue.asScala.groupBy(identity).mapValues(_.size)
        for ((eventType, count) <- counts) {
            BigtableUtils.setOrUpdateCount(
                bigtableConnection,
                tableName,
                bucketName,
                eventType,
                BigtableUtils.timeNow(),
                BigtableUtils.timeNow(),
                count 
            )     
        }
    }
  } /**

   * Starts our processing of a single Pub/Sub stream.
   * Never ends.
   *
   * @param config The configuration for our job using StreamingCountsConfig.scala
   */
  def execute(config: StreamingCountsConfig) {
    // Config Bigtable

    val pipeline = setupDataflow(config)

    // Map phase: derive events, determine bucket
    val bucketedEvents = pipeline
        .apply(PubsubIO.Read.topic(config.topicName))
        .apply(ParDo.named("BucketEvents").of(bucketEvents))

    // Reduce phase: group by key(bucket) then count and store
    val bucketedEventCounts = bucketedEvents
        .apply(GroupByKey.create[String, String]())
        .apply(ParDo.named("StoreEventCounts").of(storeEventCounts))

    // Start Dataflow pipeline
    pipeline.run
  }
}
