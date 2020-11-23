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
export PROJECT=$(gcloud config get-value project)
gcloud beta dataproc clusters create graph-processing \
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

### Read from BigQuery

First, you must create a SparkSession that has the right BigQuery depenency injected, with a paragraph like this.
With the right dependencies, you can query BigQuery directly.

```
import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

from pyspark.sql import SparkSession
spark = SparkSession.builder\
  .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.18.0")\
  .getOrCreate()

# Documentation on how to read BigQuery tables from Spark: 
# https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example#pyspark
chicago_crime = spark.read.format('bigquery') \
  .option('table', 'bigquery-public-data:chicago_crime.crime') \
  .load()
```

### Load the Neo4jImportCSV Helper Class

Use this paragraph:

```
%python
# This paragraph fetches code that implements the Neo4jImportCSV helper we need in the next step
# This is effectively the same as importing a library
from urllib.request import urlopen
link = "https://raw.githubusercontent.com/moxious/gds-import/main/lib.py"
buffer = urlopen(link).read()
python_code = buffer.decode("utf-8")
exec(python_code)
```

### Process your Data

After loading the BigQuery table in, you must process the data into a series of dataframes:

* 1 dataframe per graph label, with a unique ID column
* 1 per relationship type; with 2 foreign keys corresponding to unique IDs of the nodes the relationship connects

Here's an example:

```

import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id
from pyspark.sql.functions import sha2, concat_ws

# Add unique location ID
chicago_crime = chicago_crime.withColumn("location_id", sha2(concat_ws(",", col('block'), col('latitude'), col('longitude')), 256))
chicago_crime.cache()

# Decompose the BigQuery table into a series of "node" and "relationship" tables.
# more about this here: https://neo4j.com/developer/spark/architecture/#_normalized_loading
#
# Essentially: one table per node label, one table per rel type.
node_cases = chicago_crime.select('unique_key', 'case_number', 'date', 'description', 'year')
node_location = chicago_crime.select('location_id', 'block', 'latitude', 'longitude', 'beat', 'district', 'ward')
node_casetype = chicago_crime.select('primary_type').distinct()

rel_case_AT_location = chicago_crime.select('unique_key', 'location_id')
rel_case_TYPE_casetype = chicago_crime.select('unique_key', 'primary_type')

node_cases.cache()
node_location.cache()
node_casetype.cache()
rel_case_AT_location.cache()
rel_case_TYPE_casetype.cache()
```

### Use the Helper to Write CSV

```
helper = Neo4jImportCSV(spark, "gs://my-data/", "my-dataset")

helper.write_node_batch(df=node_cases, label="Case", key='unique_key')
helper.write_node_batch(df=node_location, label='Location', key='location_id')
helper.write_node_batch(df=node_casetype, label='CaseType', key='primary_type')

helper.write_rel_batch(df=rel_case_AT_location, source_label='Case', source_key='unique_key', rel_type='AT', target_label='Location', target_key='location_id')
helper.write_rel_batch(df=rel_case_TYPE_casetype, source_label='Case', source_key='unique_key', rel_type='TYPE', target_label='CaseType', target_key='primary_type')

print("All done!")
```

### Launch a VM and import the CSV

Review the environment variables & settings in `launch-instance.sh` and then execute that script.

It will:

* Set metadata on the VM:  `neo4j_import_set=gs://my-data/my-dataset`.  This should match
the parameters given to the Neo4jImportCSV helper object.
* Tell the VM to run csv_import.py as a startup script

**Make sure to change your password as soon as the instance comes live**.