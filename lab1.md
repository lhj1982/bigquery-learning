# Setup

```
export PROJECT_ID=your-project-id
export REGION=europe-west1
export DATASET=dataflow_lab
export BUCKET=${PROJECT_ID}-dataflow-lab
```

# Authenticate
```
gcloud auth login
gcloud auth application-default login
gcloud config set project $PROJECT_ID
```

# Enable the main APIs
```
gcloud services enable \
  dataflow.googleapis.com \
  compute.googleapis.com \
  bigquery.googleapis.com \
  pubsub.googleapis.com
```

Create a Cloud Storage bucket:

gcloud storage buckets create gs://$BUCKET \
  --location=$REGION

Create a BigQuery dataset:

bq --location=$REGION mk \
  --dataset \
  $PROJECT_ID:$DATASET