#!/bin/bash

# The purpose of this script it to launch a stand-alone GCP VM that 
# runs Neo4j with graph data science.  The VM will auto-import a graph
# from the specified bucket and location, which is expected to contain
# data formatted from spark.

gcloud compute firewall-rules create allow-neo4j-bolt-https \
   --allow tcp:7473,tcp:7687 \
   --source-ranges 0.0.0.0/0 \
   --target-tags neo4j

# The results of the import script will be written to /var/log/syslog inside of the
# VM
gcloud compute instances create my-neo4j-instance \
    --image neo4j-enterprise-1-4-1-3-apoc \
    --tags neo4j \
    --image-project launcher-public \
    --metadata-from-file startup-script=import.py