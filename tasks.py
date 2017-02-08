# Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

import datetime, json, uuid, time
from functools import partial
from random import choice

#from invoke import run, task

from google.cloud import pubsub


JAR_FILE = "gcp-dataflow-example-project-0.0.0.jar"

# Selection of EventType values
COLORS = ['Red','Orange','Yellow','Green','Blue']
        

# GCP Pub/Sub Data Generator
def picker(seq):
  """
  Returns a new function that can be called without arguments
  to select and return a random color
  """
  return partial(choice, seq)

def create_event():
  """
  Returns a choice of color and builds and event
  """
  event_id = str(uuid.uuid4())
  color_choice = picker(COLORS)

  return {
    "id": event_id,
    "timestamp": datetime.datetime.now().isoformat(),
    "type": color_choice()
  }

def write_event(topic):
  """
  Returns the event and event event_payload
  """
  event_payload = create_event()
  event_json = json.dumps(event_payload)
  topic.publish(bytes(event_json, "utf-8"))
  return event_json


#@task
def generate_events(topic_name):
    """
    load demo data with python generator script for SimpleEvents
    """
    client = pubsub.Client()
    topic = client.topic(topic_name)
    assert topic.exists()

    while True:
        event_json = write_event(topic)
        print("Event sent to Pub/Sub: {}".format(event_json))
        #time.sleep(5)


#@task
def build_project(foo):
    """
    build gcp-dataflow-example-project
    and package into "fat jar" ready for Dataflow deploy
    """
    run("sbt assembly", pty=True)


#@task
def create_bigtable_table(client, region, table):
    """
    Cloud Bigtable table creation 
    """
    pass

#@task
def create_pubsub_topic(topic_name):
    """
    create our pubsub topic
    """
    client = pubsub.Client()
    topic = client.topic(topic_name)
    assert not topic.exists()
    topic.create()
    assert topic.exists()


#@task
def run_project(config_path):
    """
    Submits the compiled "fat jar" to Cloud Dataflow and
    starts Cloud Dataflow based on project settings
    """
    pass

#    run("./spark-master/bin/spark-submit \
#        --class com.snowplowanalytics.spark.streaming.StreamingCountsApp \
#        --master local[4] \
#        ./target/scala-2.10/{} \
#        --config {}".format(JAR_FILE, config_path),
#        pty=True)
