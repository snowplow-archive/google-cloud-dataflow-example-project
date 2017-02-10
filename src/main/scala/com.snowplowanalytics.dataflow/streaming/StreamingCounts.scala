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



interface PipelineOptionsWithConnection extends PipelineOptions {
    Connection getConnection()
    void setConnection(Connection value)
}

object StreamingCounts {

  /**
   * Private function to set up Dataflow
   *
   * @param config The configuration for our job using StreamingCountsConfig.scala
   */
  private def setupDataflow(config: StreamingCountsConfig): Pipeline = {
    val dataflowPipeline = {

        PipelineOptionsFactory.register(java.lang.Class[PipelineOptionsWithConnection extends com.google.cloud.dataflow.sdk.options.PipelineOptions]) 

        //needed to access bigtable connection inside DoFn's
        val bigtableConnection = BigtableConfiguration
                .setupBigtable(config.projectId, config.instanceId)

        val options = PipelineOptionsFactory.
                .as(classOf[PipelineOptionsWithConnection])

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
        c.output(new KV[String,String](e.bucket, e.`type`))
    }
  }


  /**
   * Store elements of PCollection in Bigtable. Elements are of the form: KV[bucket, KV[eventtype, count]]
   */
  def storeCounts = new DoFn[KV[String,KV[String, JLong]], PDone]() {
    @Override
    def processElement(c: DoFn[KV[String,KV[String, JLong]], PDone]#ProcessContext) {
        val bucketName = c.element.getKey
        val eventType = c.element.getValue.getKey
        val count = c.element.getValue.getValue
        val bigtableConnextion = DoFn.Contexti
            .getPipelineOptions()
            .as(classOf[PipelineOptionsWithConnection])

        BigtableUtils.setOrUpdateCount(
            bigtableConnection,
            config.tableName,
            bucketName,
            eventType,
            BigtableUtils.timeNow(),
            BigtableUtils.timeNow(),
            count.toInt
        )
    }
  }


  /**
   * function applied to count the events per type in each bucket and 
   * store the counts in Bigtable
   */
  def countBucketEvents = new DoFn[KV[String,Iterable[String]], PDone]() {
    @Override
    def processElement(c: DoFn[KV[String,Iterable[String]], PDone]#ProcessContext) {

        //produce a String PCollection from Iterable[String]
        val flattened = c.element.getValue.apply(Flatten.FlattenIterables[String]())

        //count string occurrences in String PCollection
        val counts = flattened.apply(Count.perElement[String]())

        //store counts in appropriate bucket -> KV[bucket , KV[eventType, count]]
        val toStore = new KV[String, KV[String, JLong]](c.element.getKey, counts)
        toStore.apply(ParDo.named("StoreCounts").of(storeCounts))
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
        .apply(GroupByKey.[String, String]create)
        .apply(ParDo.named("CountBucketEvents").of(countBucketEvents))

    // Start Dataflow pipeline
    pipeline.run
  }
}
