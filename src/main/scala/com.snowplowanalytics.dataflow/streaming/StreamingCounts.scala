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

/**
 * Core of the Dataflow Streaming Application
 * 1. Configuration information is brought in from StreamingCountsApp.scala
 * 2. Object sets up Dataflow, PubSub and Bigtable
 * 3. Once connections are up, Dataflow StreamingCounts stream processing starts
 * Pub/Sub     -> Dataflow               -> Bigtable
 * Raw Data    -> Stream Processing Data -> Stored in Database
 *
 */


import java.lang.{Iterable => JIterable, Long => JLong}

import scala.collection.JavaConverters._

import org.joda.time.Duration

// Dataflow
import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.io.PubsubIO
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner
import com.google.cloud.dataflow.sdk.transforms.DoFn
import com.google.cloud.dataflow.sdk.transforms.ParDo
import com.google.cloud.dataflow.sdk.transforms.PTransform
import com.google.cloud.dataflow.sdk.transforms.GroupByKey
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows
import com.google.cloud.dataflow.sdk.transforms.windowing.Window
import com.google.cloud.dataflow.sdk.values.KV
import com.google.cloud.dataflow.sdk.values.PDone
import com.google.cloud.dataflow.sdk.values.PCollection

// Bigtable
import com.google.cloud.bigtable.dataflow.AbstractCloudBigtableTableDoFn
import com.google.cloud.bigtable.dataflow.CloudBigtableTableConfiguration
import com.google.cloud.bigtable.dataflow.CloudBigtableTableConfiguration.Builder

//HBase
import org.apache.hadoop.hbase.client.Connection

// This project
import storage.BigtableUtils

trait PipelineOptionsWithBigtable extends DataflowPipelineOptions {
    def getTablename() : String
    def setTablename(value: String) : Unit
}

object StreamingCounts {

  /**
   * Private function to set up Dataflow and Bigtable
   *
   * @param config The configuration for our job using StreamingCountsConfig.scala
   */
  private def setupDataflowAndBigtable(config: StreamingCountsConfig): (Pipeline, CloudBigtableTableConfiguration) = {
    PipelineOptionsFactory.register(classOf[PipelineOptionsWithBigtable])

    val options = PipelineOptionsFactory
      .as(classOf[PipelineOptionsWithBigtable])

    options.setRunner(classOf[DataflowPipelineRunner])
    options.setProject(config.projectId)
    options.setStagingLocation(config.stagingLocation)
    options.setStreaming(true)

    val bigtableConfig = new CloudBigtableTableConfiguration.Builder()
        .withProjectId(config.projectId)
        .withInstanceId(config.instanceId)
        .withTableId(config.tableName)
        .build()

    val pipeline = Pipeline.create(options)

    (pipeline, bigtableConfig)
  }

  /**
   * function applied in the "map" phase
   */
  def bucketEvents = new DoFn[String,KV[String,String]]() {
    
    override def processElement(c: DoFn[String,KV[String,String]]#ProcessContext) {
        println(c.element)
        val e = SimpleEvent.fromJson(c.element)
        c.output(KV.of(e.bucket, e.`type`))
    }
  }

  /**
   * transform applied to count the events per type in each bucket and 
   * store the counts in Bigtable
   */

  class StoreEventCounts(config: CloudBigtableTableConfiguration) 
            extends PTransform[PCollection[KV[String, JIterable[String]]], PDone] {

    def storeEventCounts(config: CloudBigtableTableConfiguration) = new AbstractCloudBigtableTableDoFn[KV[String,JIterable[String]], String](config) {

      override def processElement(c: DoFn[KV[String,JIterable[String]], String]#ProcessContext) {
          val bucketName = c.element.getKey
          val tableName = c
              .getPipelineOptions
              .as(classOf[PipelineOptionsWithBigtable])
              .getTablename()

          val counts = c.element.getValue.asScala.groupBy(identity).mapValues(_.size)
          for ((eventType, count) <- counts) {
              BigtableUtils.setOrUpdateCount(
                  getConnection,
                  tableName,
                  bucketName,
                  eventType,
                  BigtableUtils.timeNow(),
                  BigtableUtils.timeNow(),
                  count 
              )     
          }
          c.output("dummy output")
      }
    } 

    override def apply(counts : PCollection[KV[String, JIterable[String]]]) : PDone = {
        counts.apply(ParDo.named("StoreEventCounts").of(this.storeEventCounts(config)))
        PDone.in(counts.getPipeline)
    }
  }


/**

   * Starts our processing of a single Pub/Sub stream.
   * Never ends.
   *
   * @param config The configuration for our job using StreamingCountsConfig.scala
   */
  def execute(config: StreamingCountsConfig) {
    // Config Bigtable

    val (pipeline, bigtableConfig) = setupDataflowAndBigtable(config)

    // Map phase: derive events, determine bucket
    val bucketedEvents = pipeline
        .apply(PubsubIO.Read.topic(config.topicName))
        .apply(ParDo.named("BucketEvents").of(bucketEvents))
        .apply(Window.into[KV[String,String]](FixedWindows.of(Duration.standardMinutes(1))))

    // Reduce phase: group by key(bucket) then count and store
    val bucketedEventCounts = bucketedEvents
        .apply(GroupByKey.create[String, String]())
        .apply(new StoreEventCounts(bigtableConfig))

    // Start Dataflow pipeline
    pipeline.run
  }
}
