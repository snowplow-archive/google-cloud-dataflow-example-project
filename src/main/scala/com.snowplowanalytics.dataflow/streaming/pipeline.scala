/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.dataflow.example

import java.lang.{Long => JLong}
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream

import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.io.PubsubIO
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner
import com.google.cloud.dataflow.sdk.transforms.Count
import com.google.cloud.dataflow.sdk.transforms.DoFn
import com.google.cloud.dataflow.sdk.transforms.MapElements
import com.google.cloud.dataflow.sdk.transforms.ParDo
import com.google.cloud.dataflow.sdk.transforms.SimpleFunction
import com.google.cloud.dataflow.sdk.values.KV
import com.google.cloud.bigtable.dataflow.CloudBigtableScanConfiguration
import com.google.cloud.bigtable.dataflow.CloudBigtableIO

import org.apache.hadoop.hbase.client.{Mutation, Put}
import org.apache.hadoop.hbase.io


object MinimalWordCount {

  def formatToHBase = new DoFn[KV[String,JLong],Mutation]() {
    @Override
    def processElement(c: DoFn[KV[String,JLong],Mutation]#ProcessContext) {
        c.output(new Put(c.element.getKey.getBytes)
                    .addColumn("cf1".getBytes, "SimpleEvent".getBytes, (" "+c.element.getValue).getBytes))
    }
  }

  def extractWords = new DoFn[String,String]() {
    @Override
    def processElement(c: DoFn[String,String]#ProcessContext) {
        for (word <- c.element().split("[^a-zA-Z']+")) {
            if (!word.isEmpty()) {
                c.output(word)
            }
        }
    }
  }

  def main(args: Array[String]) {

    // Config Bigtable
    val config = new CloudBigtableScanConfiguration.Builder()
    .withProjectId("gcp-dataflow-example")
    .withInstanceId("gcp-dataflow-example-instance")
    .withTableId("test-table")
    .build

    // Config Pipeline
    val options = PipelineOptionsFactory.fromArgs(args).withValidation().as(classOf[DataflowPipelineOptions])
    options.setRunner(classOf[BlockingDataflowPipelineRunner])
    options.setProject("gcp-dataflow-example")
    options.setStagingLocation("gs://gcp-dataflow-example-bucket/staging")

    // Create the Pipeline object with the options we defined above.
    val p = Pipeline.create(options)
    CloudBigtableIO.initializeForWrite(p)

    p.apply(PubsubIO.Read.topic("topic1"))
     .apply(ParDo.named("ExtractWords").of(extractWords))
     .apply(Count.perElement[String]())
     .apply(ParDo.named("FormatToHBase").of(formatToHBase))
     .apply(CloudBigtableIO.writeToTable(config))

    // Run the pipeline.
    p.run
  }
}
