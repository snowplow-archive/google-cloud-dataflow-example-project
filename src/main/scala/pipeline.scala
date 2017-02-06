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

import java.lang.{Long => JLong}

object MinimalWordCount {
  

  def SimpleFunctionInstance = new SimpleFunction[KV[String,JLong],String]() {
    @Override
    def apply(input: KV[String, JLong]) : String = {
        input.getKey() + ": " + input.getValue()
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
    val options = PipelineOptionsFactory.fromArgs(args).withValidation().as(classOf[DataflowPipelineOptions])

    options.setRunner(classOf[BlockingDataflowPipelineRunner])
    options.setProject("gcp-dataflow-example")
    options.setStagingLocation("gs://gcp-dataflow-example-bucket/staging")

    // Create the Pipeline object with the options we defined above.
    val p = Pipeline.create(options)

    //p.apply(PubsubIO.Read.topic("test-input-topic"))
    p.apply(TextIO.Read.from("gs://dataflow-samples/shakespeare/*"))
     .apply(ParDo.named("ExtractWords").of(extractWords))
     .apply(Count.perElement[String]())
     .apply("FormatResults", MapElements.via(SimpleFunctionInstance))
     .apply(TextIO.Write.to("gs://gcp-dataflow-example-bucket/out"))
     //.apply(PubsubIO.Write.topic("test-output-topic"))

    // Run the pipeline.
    p.run()
  }
}
