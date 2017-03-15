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

import java.lang.{Iterable => JIterable}

import scala.collection.JavaConverters._

import org.apache.log4j.BasicConfigurator

import org.joda.time.Duration

// Dataflow
import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.io.PubsubIO
import com.google.cloud.dataflow.sdk.options.{DataflowPipelineOptions, PipelineOptionsFactory}
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner
import com.google.cloud.dataflow.sdk.transforms.{DoFn, ParDo, PTransform, GroupByKey}
import com.google.cloud.dataflow.sdk.transforms.windowing.{FixedWindows, Window}
import com.google.cloud.dataflow.sdk.values.{KV, PDone, PCollection}

// Bigtable
import com.google.cloud.bigtable.dataflow.{AbstractCloudBigtableTableDoFn, CloudBigtableTableConfiguration}
import com.google.cloud.bigtable.dataflow.CloudBigtableTableConfiguration.Builder

// This project
import storage.BigtableUtils

 /**
  * Core of the Dataflow Streaming Application
  * 1. Configuration information is brought in from StreamingCountsApp.scala
  * 2. Object sets up Dataflow, PubSub and Bigtable
  * 3. Once connections are up, Dataflow StreamingCounts stream processing starts
  * Pub/Sub   -> Dataflow         -> Bigtable
  * Raw Data  -> Stream Processing Data -> Stored in Database
  *
  * Transform applied to count the events per type in each bucket and 
  * store the counts in Bigtable
  */
class StreamingCounts(config: CloudBigtableTableConfiguration, tableName: String, columnFamily: String)
  extends PTransform[PCollection[KV[String, JIterable[String]]], PDone] {

  override def apply(counts : PCollection[KV[String, JIterable[String]]]): PDone = {
    counts.apply(ParDo.named("StoreEventCounts").of(
      StreamingCounts.storeEventCounts(config, tableName, columnFamily)))
      PDone.in(counts.getPipeline)
  }
}

object StreamingCounts {
  /**
   * Function to set up Bigtable
   *
   * @param config The configuration for our job using StreamingCountsConfig
   * @return A Cloud Bigtable configuration instance with the options provided in config
   */
  private def setupBigtable(config: StreamingCountsConfig): CloudBigtableTableConfiguration =
    new CloudBigtableTableConfiguration.Builder()
      .withProjectId(config.projectId)
      .withInstanceId(config.instanceId)
      .withTableId(config.tableName)
      .build()

  /**
   * Function to set up Dataflow
   *
   * @param config The configuration for our job using StreamingCountsConfig
   * @param bigtableConfig The Bigtable config obtained from running setupBigtable
   * @return a Pipeline instance, to which the transforms will be applied
   */
  private def setupDataflow(config: StreamingCountsConfig, 
    bigtableConfig: CloudBigtableTableConfiguration): Pipeline = {
 
    val options = PipelineOptionsFactory
      .as(classOf[DataflowPipelineOptions])
 
    options.setRunner(classOf[DataflowPipelineRunner])
    options.setProject(config.projectId)
    options.setStagingLocation(config.stagingLocation)
    options.setStreaming(true)
    Pipeline.create(options)
  }

  /**
   * Function applied in the "map" phase. Events that aren't properly formatted are discarded
   */
  def bucketEvents = new DoFn[String, KV[String, String]]() {

    override def processElement(c: DoFn[String, KV[String, String]]#ProcessContext): Unit = {
      val eventOpt = SimpleEvent.fromJson(c.element) // Option[SimpleEvent]
      val kvOpt = eventOpt.map(e => KV.of(e.bucket, e.`type`)) // Option[KV[String, String]]
      kvOpt.foreach(c.output) // If the option is defined the lambda in the foreach will be executed 
    }
  }

  /**
   * Extends DoFn to implement event storage.
   * Makes use of a DoFn subclass (AbstractCloudBigtableTableDoFn),
   * that allows a Bigtable configuration instance to be passed and accessed inside.
   * Even though this transform outputs nothing, we can't output a single PDone element,
   * and this DoFn needs a defined a serializable output type. We chose String, but it 
   * could have been anything else.
   *
   * @param config The Bigtable configuration instance
   * @param tableName The Bigtable table name in which the events will be stored
   * @param columnFamily The Bigtable column family to be used
   * @return A DoFn instance to be used inside a ParDo transform
   */
  def storeEventCounts(config: CloudBigtableTableConfiguration, tableName: String, columnFamily: String) = 
    new AbstractCloudBigtableTableDoFn[KV[String, JIterable[String]], String](config) {

    override def processElement(c: DoFn[KV[String, JIterable[String]], String]#ProcessContext): Unit = {
      val bucketName = c.element.getKey
 
      val counts = c.element.getValue.asScala.groupBy(identity).mapValues(_.size)
      for ((eventType, count) <- counts) {
        BigtableUtils.setOrUpdateCount(
          getConnection,
          tableName,
          columnFamily, 
          bucketName,
          eventType,
          BigtableUtils.timeNow(),
          BigtableUtils.timeNow(),
          count 
        )   
      }
    }
  }

  /**
   * Starts our processing of a single Pub/Sub stream.
   * Never ends.
   *
   * @param config The configuration for our job using StreamingCountsConfig.scala
   */
  def execute(config: StreamingCountsConfig): Unit = {
    // Config Bigtable
    BasicConfigurator.configure
    val bigtableConfig = setupBigtable(config)
    val pipeline = setupDataflow(config, bigtableConfig)

    // Map phase: derive events, determine bucket
    val bucketedEvents = pipeline
      .apply(PubsubIO.Read.topic(config.topicName))
      .apply(ParDo.named("BucketEvents").of(bucketEvents))
      .apply(Window.into[KV[String, String]](FixedWindows.of(Duration.standardMinutes(1))))

    // Reduce phase: group by key(bucket) then count and store
    val bucketedEventCounts = bucketedEvents
      .apply(GroupByKey.create[String, String]())
      .apply(new StreamingCounts(bigtableConfig, config.tableName, config.columnFamily))

    // Start Dataflow pipeline
    pipeline.run
  }
}
