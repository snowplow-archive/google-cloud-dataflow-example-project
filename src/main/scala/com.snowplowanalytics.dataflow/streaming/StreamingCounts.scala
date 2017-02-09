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
object StreamingCounts {

  /**
   * Private function to set up Dataflow
   *
   * @param config The configuration for our job using StreamingCountsConfig.scala
   */
  private def setupDataflow(config: StreamingCountsConfig): StreamingContext = {
    val dataflowPipeline = {
        val options = PipelineOptionsFactory.fromArgs(args)
                .withValidation().as(classOf[DataflowPipelineOptions])
        options.setRunner(classOf[BlockingDataflowPipelineRunner])
        options.setProject(config.project)
        options.setStagingLocation(config.stagingPath)

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

  def storeCounts = new DoFn[KV[String, KV[String, JLong]], PDone]() {
    @Override
    def processElement(c: DoFn[KV[String,KV[String, JLong]], PDone]#ProcessContext) {
       c.output(new Put( 
    }
  }


  /**
   * function applied to count the events per type in each bucket and 
   * store the counts in Bigtable
   */
  def countBucketEvents = new DoFn[KV[String,Iterable[String]], PDone]() {
    @Override
    def processElement(c: DoFn[KV[String,Iterable[String]], PDone]#ProcessContext) {
        val counts = c.element.getValue.apply(Count.perElement[String]())
    }
  } /**

   * Starts our processing of a single Pub/Sub stream.
   * Never ends.
   *
   * @param config The configuration for our job using StreamingCountsConfig.scala
   */
  def execute(config: StreamingCountsConfig) {
    // Config Bigtable
    val bigTableConfig = new CloudBigtableScanConfiguration.Builder()
    .withProjectId(config.project)
    .withInstanceId(config.bigtable.instance)
    .withTableId(config.bigtable.table)
    .build

    val pipeline = setupDataflow(config)

    // Map phase: derive events, determine bucket
    val bucketedEvents = pipeline
        .apply(PubsubIO.Read.topic(config.topic))
        .apply(ParDo.named("BucketEvents").of(bucketEvents))

    // Reduce phase: group by key then by count
    val bucketedEventCounts = bucketedEvents
        .apply(GroupByKey.[String, String]create)
        .apply(ParDo.named("CountBucketEvents").of(countBucketEvents))

    // Save the records in Bigtable
    bucketedEventCounts
        .apply(CloudBigtableIO.writeToTable(bigTableConfig))

    // Start Dataflow pipeline
    pipeline.run
  }
}
