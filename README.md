# GDS Import for Neo4j

The purpose of this repo is to speed up data processing & ingest into Neo4j instances for GDS processing.

The example we'll use supposes you want to import data from BigQuery into Neo4j.

## Approach

- Extract data in any form from Google BigQuery using SQL, and DataProc
- Process the data in any way needed using Spark / SQL tools into Neo4j's
[import CSV file format](https://neo4j.com/docs/operations-manual/current/tools/import/file-header-format/)
- Store the result to Google Cloud Storage
- Launch a VM with a loading script, pointing it to the GCS bucket where the results sit.  This script will
run the `neo4j-admin import` command on the CSV, and then start Neo4j as normal.

## Before you Begin -- Requirements

### APIs we'll need

Ensure you have service accounts & permissions configured to use all of the following.

- Cloud Storage (to hold data)
- Dataproc (to process results)
- BigQuery (as a data source)
- Compute Engine (to run the resulting Neo4j graph with GDS)

### Google Dataproc

The components we need are Spark > 2.4.5 & Scala 2.11 or 2.12, which can be found 
in the dataproc images version 1.5.x.

Example suitable cluster:

**Make sure to use the GCS_CONNECTOR_VERSION metadata; it helps install the cloud storage connector you'll need**

```
export REGION=us-east1
export PROJECT=testbed-187316
gcloud beta dataproc clusters create spark-cluster \
    --enable-component-gateway \
    --metadata GCS_CONNECTOR_VERSION=2.0.0 \
    --region $REGION \
    --master-machine-type n1-standard-4 \
    --master-boot-disk-size 500 --num-workers 2 \
    --worker-machine-type n1-standard-4 \
    --worker-boot-disk-size 500 \
    --image-version 1.5-debian10 \
    --optional-components ZEPPELIN \
    --project $PROJECT
```

### Dependent JARs

Run this paragraph:

```
%sh
rm neo4j*.jar spark-bigquery-latest.jar
wget https://storage.googleapis.com/neo4j-connector-apache-spark/neo4j-connector-apache-spark_2.12-4.0.0.jar
wget https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-latest.jar
```

Add the two resulting JAR files as dependencies to your Zeppelin's pyspark interpreter, then save & 
restart the interpreter.

- Cloud Storage Connector for Spark

If you used the initialization metadata documented above, this JAR is already installed into the cluster for
you.



