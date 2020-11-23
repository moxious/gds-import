#!/usr/bin/python3
#
# This script grabs data from google storage, processess the sub-files according to
# an expected structure, and calls neo4j-admin import on the result to dynamically import
# a CSV-specified graph.
#
# See the spark examples in this repo; the format of the CSV is expected to come from the
# Neo4jImportCSV python class run in Spark.
#
# This script is further intended to be a VM startup script for Neo4j.  Upon VM launch,
# we want to write a graph from the original CSV files, and then start Neo4j.
from os import listdir, mkdir
from os.path import isfile, join, isdir
import sys
import subprocess
import os

import_args = ["neo4j-admin", "import"]

def csvFilesInDirectory(dir):    
    files = listdir(dir)
    csvFiles = list(filter(lambda e: isfile(e) and e.endswith('.csv'), [join(dir, f) for f in files]))
    return csvFiles

def prepare_node(label, dir):
    csv_files = csvFilesInDirectory(dir)
    header_dir = join(dir, "headers")
    print("Checking for %s headers in %s" % (label, header_dir))
    header_files = csvFilesInDirectory(header_dir)
    print("header_files %s" % header_files)
    if len(header_files) == 0 or len(header_files) > 1:
        raise Exception("Missing or too many header files for %s => %s" % (label, header_files))

    headers = header_files[0]

    # Format example:  --nodes=Customer=customer_headers.csv,data1.csv,data2.csv
    return "--nodes=%s=%s,%s" % (label, headers, ",".join(csv_files))

def prepare_rel(rel_type, dir):
    csv_files = csvFilesInDirectory(dir)
    header_dir = join(dir, "headers")
    print("Checking for %s headers in %s" % (rel_type, header_dir))
    header_files = csvFilesInDirectory(header_dir)

    if len(header_files) == 0 or len(header_files) > 1:
        raise Exception("Missing or too many header files for rel type %s => %s" % (rel_type, header_files))

    headers = header_files[0]

    # Format example:  --relationships=ORDERED="customer_orders_header.csv,orders1.csv,orders2.csv"
    return "--relationships=%s=%s,%s" % (rel_type, headers, ",".join(csv_files))

def copy_data_from_storage(import_from):
    staging = "staging"
    if not os.path.exists(staging):
        mkdir(staging)
    result = subprocess.run([
        "gsutil", "-m", "rsync", "-r", import_from, staging
    ], stderr=subprocess.STDOUT)

    print("Finished copying with %s" % result)

    print("Files available are %s" % listdir(staging))
    return staging

def main():
    dir = copy_data_from_storage("gs://meetup-data/chicago_crime_bigquery")

    data_dirs = [f for f in listdir(dir) if isdir(join(dir, f))]
    print(data_dirs)

    nodes = list(filter(lambda e: e.startswith("node_"), data_dirs))
    rels = list(filter(lambda e: e.startswith("rel_"), data_dirs))
    print(nodes)
    print(rels)

    for node_dir in nodes:
        label=node_dir.replace("node_", "")
        import_args.append(prepare_node(label, join(dir, node_dir)))

    for rel_dir in rels:
        rel_type=rel_dir.replace("rel_", "")
        import_args.append(prepare_rel(rel_type, join(dir, rel_dir)))

    command=" ".join(import_args)
    print(command)
    result = subprocess.run(import_args, stderr=subprocess.STDOUT)
    print("Finished neo4j-admin import");

try:
    main()
except Exception as err:
    print(err)
    print("Unhandled exception in import process")
    sys.exit(1)


#onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]