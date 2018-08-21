**This is not an official GCP repository**

# Sample Dataflow Pipeline
## Overview
This project contains a sample data pipeline featuring Google Cloud's Pub/Sub, Dataflow, and BigQuery products. The solution will simulate calculating a windowed average of data received through Pub/Sub and processed with Dataflow. The result will be stored in a BigQuery table.

## Setup Project
1. Create new GCP project
2. Open the Google Cloud Shell
3. Clone this project
```
git clone https://github.com/tfrantzen/sample-dataflow-aggregations.git
```
4. Enable the Pub/Sub API
```
gcloud services enable pubsub.googleapis.com
```
5. Enable the Dataflow API
```
gcloud services enable dataflow.googleapis.com
```

## Setup Pub/Sub
1. Open the Google Cloud Shell
2. Create a new Pub/Sub topic
```
export PUBSUB_TOPIC=<pubsub-topic>
gcloud pubsub topics create $PUBSUB_TOPIC
```
3. Create a new Pub/Sub subscription
```
export PUBSUB_SUBSCRIPTION=<pubsub-subscription>
gcloud pubsub subscriptions create --topic $PUBSUB_TOPIC $PUBSUB_SUBSCRIPTION
```
## Setup BigQuery
1. Open the Google Cloud Shell
2. Create new dataset
```
export BIGQUERY_DATASET=data
bq mk $BIGQUERY_DATASET
```

## Setup Dataflow
1. Open the Google Cloud Shell
2. Create new Google Cloud Storage bucket
```
gsutil mb gs://<bucket_name>
```
3. Change directories to the folder containing the Constants.java file
```
cd ~/sample-dataflow-aggregations/dataflow-pipeline/src/main/java/com/ceaas/pubsub2bq/shared
```
4. Update Constants.java file using your favorite editor. Update the following:
```
	public static final String GCS_BUCKET_NAME="<bucket-name>";
	public static final String PROJECT_ID="<project-id>";
	public static final String PUBSUB_SUBSCRIPTION="<pubsub-subscription>";
```
5. Change directories to dataflow-pipeline folder
```
cd ~/sample-dataflow-aggregations/dataflow-pipeline
```
6. Deploy Dataflow job
```
./runOnDataflow.sh
```

## Run the Pub/Sub Simulator
1. Open the Google Cloud Shell
2. Change directories to the Pub/Sub Simulator
```
cd ~/sample-dataflow-aggregations/pubsub-simulator/
```
3. Install Dependencies
```
npm install
```
4. Create service account key
```
gcloud iam service-accounts create service-account.json
gcloud iam service-accounts keys create ~/service-account.json --iam-account service-account@$(gcloud config get-value project).iam.gserviceaccount.com
```
5. Set GOOGLE_APPLICATION_CREDENTIALS to the service account
```
set GOOGLE_APPLICATION_CREDENTIALS=service-account.json
```
6. Edit index.js using your favorite editor. Update the following:
```
const projectId = '<projectId>';
const pubsubTopicName = '<pubsubTopicName>';
```
7. Run the Simulator
```
node index.js
```
