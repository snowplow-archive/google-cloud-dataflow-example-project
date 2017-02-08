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
  def bucketEvents = new DoFn[String,(String,String)]() {
    @Override
    def processElement(c: DoFn[String,(String,String)]#ProcessContext) {
        val e = SimpleEvent.fromJson(c.element)
        c.output((e.bucket, e.`type`))
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
      .groupByKey
      .map { case (eventType, events) =>
        val count = events.groupBy(identity).mapValues(_.size)
        (eventType, count)
      }

    // Iterate over each aggregate record and save the record into DynamoDB
    bucketedEventCounts.foreachRDD { rdd =>
      rdd.foreach { case (bucket, aggregates) =>
        aggregates.foreach { case (eventType, count) =>
          DynamoUtils.setOrUpdateCount(
            dynamoConnection,
            config.tableName,
            bucket.toString,
            eventType,
            DynamoUtils.timeNow(),
            DynamoUtils.timeNow(),
            count.toInt
          )
        }
      }
    }

    // Start Spark Streaming process
    streamingSparkContext.start()
    streamingSparkContext.awaitTermination()
  }
}
