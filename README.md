# GCP Dataflow Example Project

## Introduction

This is a simple stream processing example project, written in Scala and using Google Cloud Platform's services/APIs:

- Dataflow for data processing
- Pub/Sub for communication
- Bigtable for storage

**Running this requires a Google account and enabling GCP services, which will incur certain costs**

We assume you have [pyinvoke](http://www.pyinvoke.org/) installed, as well as the [Google Cloud Python SDK](https://pypi.python.org/pypi/google-cloud), in order for the helper script to work.

## Overview

We have implemented a super simple analytics-on-write stream processing job using Dataflow. Our Dataflow job reads a Pub/Sub topic containing events in a JSON format:

```json
{
  "timestamp": "2015-06-05T12:54:43.064528",
  "type": "Green",
  "id": "4ec80fb1-0963-4e35-8f54-ce760499d974"
}
```

Our job counts the events by `type` and aggregates these counts into 1-minute buckets. The job then takes these aggregates and saves them into a table in Bigtable.

## Tutorial

### 1. Setting up GCP

First we need to create and setup a GCP project. To do so, you can follow [this guide on Snowplow's wiki](https://github.com/snowplow/snowplow/wiki/GCP:-Getting-Started). Following those instructions, you'll need to enable the following APIs/services:

- Dataflow
- Bigtable
- Pub/Sub
- Storage

After that, you'll need to authenticate the computer where you'll be running the project using the Google Cloud SDK. To do so, run the following commands:

```bash
$ gcloud auth login
$ gcloud auth application-default login
```

Note: Some of the following tasks can be performed using the helper script provided in this repo. The script uses pyinvoke - each action provided by the script is preceded by 'inv'. Run ``` $ inv --list ``` on the repo root to list the available actions on the script.  At some important steps the script assumes default configurations, which might not be the right ones for you. For example, when creating a bigtable instance and table, the script will choose SSD storage over HDD, which is much more expensive. 

#### Cloud Pub/Sub

After enabling Pub/Sub, go to https://console.cloud.google.com/cloudpubsub?project=YOUR-PROJECT-ID . Note that you have to change *YOUR-PROJECT-ID* to, you guessed it, your project id.

There, click "Create Topic" and type in whatever name you want to give to the topic we'll be using to read the events from. In our example, we used "test-topic". Keep track of your topic's name (you can always go to that URL to check it), as it will be needed when you're creating your config file.

![creating a pubsub topic][create-topic]

#### Cloud Storage
Create a staging location in which the system will store the jars ran by the workers. To do this, you need to enable the Storage API.

After that, go to https://console.cloud.google.com/storage/browser?project=YOUR-PROJECT-ID . Click "Create Bucket". Fill in the appropriate details. As this is an example project, we suggest you pick the cheapest option.

After creating your bucket, you might want to create a folder inside it, to serve as your staging location. You can also just use the bucket's root. To create a folder in your bucket, after you created the bucket, select it in the list and then click "Create Folder".

Your staging location will be:

```bash
gs://your-bucket-name/the-folder-that-you-might-have-created
```

![setting staging location][bucket1]

![setting staging location][bucket2]

#### Bigtable

After enabling Bigtable, we'll need to create an instance where our table will be stored. And then create the actual table.

##### Creating an instance

Go to https://console.cloud.google.com/bigtable/instances?project=YOUR-PROJECT-ID , again taking care of changing it with your project id. Click "Create instance" and fill in the appropriate details. The name is, again, important. We used "test-instance" in our example. We suggest you to select HDD as storage type as it is much cheaper, and this is for testing purposes only.
                  
|                                            |                                            |
|:------------------------------------------:|:------------------------------------------:|
| ![creating a bigtable instance][bigtable1] | ![creating a bigtable instance][bigtable2] |

##### Creating a table

We usually do this through an HBase shell inside Google Cloud Shell ([reference](https://cloud.google.com/bigtable/docs/quickstart-hbase)).  

Go to your Google Cloud Dashboard (https://console.cloud.google.com/home/dashboard?project=YOUR-PROJECT-ID) and click on the little terminal symbol on the top right corner. That will boot your Google Shell. Wait a bit and then run the following commands:

```bash
$  curl -f -O https://storage.googleapis.com/cloud-bigtable/quickstart/GoogleCloudBigtable-Quickstart-0.9.5.1.zip
$ unzip GoogleCloudBigtable-Quickstart-0.9.5.1.zip
$ cd quickstart
$ ./quickstart.sh
```

This will boot the HBase shell. You'll then issue a ```create``` command, followed by 2 quoted arguments. These are the table name, and the column family. In Bigtable, a column family is a set of columns that are related and likely to be accessed inside a small time interval (which allows some storage optimization and caching to take place). In the context of this project, we only have one column family.

In our case, we used "test-table" as the table name and "cf1" as the column family:

```
hbase-shell> create "test-table", "cf1"
```

To see the data written in the table, later, you'll run the following command (again, inside the HBase shell):

```
hbase-shell> scan "test-table"
```

### 2. Sending events to Pub/Sub

At this step, you've already created a Cloud Pub/Sub topic, either via the web interface or the helper script we provide in this repo. In our case, the topic is called "test-topic". You can also specify the number of events to send. By default, infinite events will be sent.

To start the event generator, simply run the following command, on the repo's root:

```bash
$ inv generate_events --topic-name=test-topic --nr-events=5
```

### 3. Running our job on Dataflow

You can either go to Bintray ([link](https://bintray.com/snowplow/snowplow-generic/google-cloud-dataflow-example-project)) 
and download a fat jar assembled by us, or build it yourself (if you have Scala and SBT installed).
A fat jar is a jar file that contains all the needed dependencies to run.

To build it, run ```$ sbt assembly```, on the repo root.

You can then either enter SBT's REPL and run it from there, doing: 

```bash
sbt-repl> run --config /path/to/config/file.hocon
```

Or you can use the helper script, from the repo root:

```bash
$ inv run_project --config=/path/to/config/file.hocon
```

There's an example config file in [config/config.hocon.sample](https://raw.githubusercontent.com/snowplow/google-cloud-dataflow-example-project/release/0.1.0/config/config.hocon.sample) . If you've been using the same names as we did, you'll only need to perform some minimal changes (namely: the project ID; adjusting the Pub/Sub topic full name; setting Dataflow's correct staging location)

### 4. Monitoring your job

Now there are 2 places we want to check to see if everything is running smoothly.

The first place is the Dataflow's web interface. Go to https://console.cloud.google.com/dataflow?project=YOUR-PROJECT-ID and select the job you just submitted. You should then see a graph with the several transforms that make up our data pipeline. You can click on the transforms to get specific info about each one, such as their throughput. If something is not working properly, you'll get warnings under "Logs". You can also check the central log in: https://console.cloud.google.com/logs?project=YOUR-PROJECT-ID

![running job on dataflow][dataflow]

The second place you'll want to check is your Bigtable table. To do so, as we've described before, run the following command inside the HBase shell on your Google Cloud Shell (assuming your table is called "test-table"):

```
hbase-shell> scan "test-table"
```

This will print the several rows in the table, which correspond to the counts of a specific type of event in a specific time-bucket.

![bigtable table rows][result]

### 5. Cleaning Up

These steps will stop your job execution:

- Click "Stop Job" on Dataflow's menu, under "Job Status", on the right sidebar. Then select "Cancel".
- Kill the event generator (Ctrl+C on the corresponding terminal window)
- On Google Cloud Shell, inside the HBase shell, run:

```bash
hbase-shell> disable "test-table"
hbase-shell> drop "test-table"
```

To avoid unwanted spendings, you'll also want to bring your resources down. The easiest way to do it, is to just delete the project you've been working on. To do so, go [here](https://console.cloud.google.com/iam-admin/projects), select the project and click "Delete Project".

## Copyright and license

Copyright 2017 Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0] [license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[create-topic]: screenshots/create-topic.png
[bucket1]: screenshots/bucket1.png
[bucket2]: screenshots/bucket2.png
[bigtable1]: screenshots/bigtable1.png
[bigtable2]: screenshots/bigtable2.png
[dataflow]: screenshots/dataflow.png
[result]: screenshots/result.png
[license]: LICENSE-2.0.txt
