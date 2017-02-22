# Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
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

from invoke import run, task

from google.cloud import pubsub
from google.cloud import bigtable
import os.path

JAR_FILE = "gcp-dataflow-streaming-example-project-0.1.0.jar"

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
  topic.publish(event_json.encode("utf-8"))
  return event_json

@task
def generate_events(ctx, topic_name, nr_events=0):
  """
  Load demo data with python generator script for SimpleEvents
  """
  client = pubsub.Client()
  topic = client.topic(topic_name)
  assert topic.exists()
  cnt = 1

  while True:
    event_json = write_event(topic)
    print("Event sent to Pub/Sub: {}".format(event_json))
    if nr_events != 0:
      if cnt == nr_events:
        return
      cnt += 1

@task
def build_project(ctx):
  """
  Build gcp-dataflow-example-project and package into "fat jar" ready for Dataflow deploy
  """
  run("sbt assembly", pty=True)

@task
def create_bigtable_table(ctx, region="us-west1-a", instance_id="test-instance", table_name="test-table", column_family="cf1"):
  """
  Cloud Bigtable table (and instance) creation. Check the full docstring for details!
  Setting display_name to the same as the instance_id by default. Assuming non-existent instance!
  """
  client = bigtable.Client(admin=True)
  instance = client.instance(instance_id, region, display_name=instance_id)
  instance.create()
  table = instance.table(table_name)
  table.create()
  column_family = table.column_family(column_family)
  column_family.create()
  

@task
def create_pubsub_topic(ctx, topic_name):
  """
  Create our pubsub topic
  """
  client = pubsub.Client()
  topic = client.topic(topic_name)
  assert not topic.exists()
  topic.create()
  assert topic.exists()

@task
def run_project(ctx, config, fat_jar_path=JAR_FILE):
  """
  Submits the compiled "fat jar" to Cloud Dataflow and starts Cloud Dataflow based on project settings
  """
  if os.path.isfile(fat_jar_path):
    if os.path.isfile(config):  
      run("java -Dconfig.file={} -jar {}".format(config, fat_jar_path), pty=True)
    else:
      print("Invalid or non-existent config file.")
  else:
    print("Invalid or non-existent fat jar.")
