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
from datetime import datetime
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

def log(msg):
    print("%s csv_import: %s" % (datetime.now().isoformat(), msg))

def copy_data_from_storage(import_from):
    staging = "staging"
    if not os.path.exists(staging):
        mkdir(staging)
    result = subprocess.run([
        "gsutil", "-m", "rsync", "-r", import_from, staging
    ], stderr=subprocess.STDOUT)

    log("Finished copying with %s" % result)
    log("Files available are %s" % listdir(staging))
    return staging

def catcher(fn, message):
    try:
        return fn()
    except Exception as err:
        log("Failed to %s: %s" % (message, err))

def start_neo4j():
    return catcher(lambda: subprocess.run(['systemctl', 'start', 'neo4j']), "start neo4j system service")

def stop_neo4j():
    return catcher(lambda: subprocess.run(['systemctl', 'stop', 'neo4j']), "stop neo4j system service")

def assign_permissions():
    return catcher(
        lambda: subprocess.run(["chown", "-R", "neo4j:neo4j", "/var/lib/neo4j/data/*"], shell=True), 
        "assign neo4j:neo4j privileges to restored database")

def main():
    stop_neo4j()
    dir = copy_data_from_storage("gs://meetup-data/chicago_crime_bigquery")

    data_dirs = [f for f in listdir(dir) if isdir(join(dir, f))]
    log(data_dirs)

    nodes = list(filter(lambda e: e.startswith("node_"), data_dirs))
    rels = list(filter(lambda e: e.startswith("rel_"), data_dirs))
    log("Nodes: %s" % nodes)
    log("Relationships: %s" % rels)

    for node_dir in nodes:
        label=node_dir.replace("node_", "")
        import_args.append(prepare_node(label, join(dir, node_dir)))

    for rel_dir in rels:
        rel_type=rel_dir.replace("rel_", "")
        import_args.append(prepare_rel(rel_type, join(dir, rel_dir)))

    command=" ".join(import_args)
    log("Running command %s" % command)
    result = subprocess.run(import_args, stderr=subprocess.STDOUT)
    print("Finished neo4j-admin import")
    assign_permissions()
    start_neo4j()

try:
    main()
    sys.exit(0)
except Exception as err:
    log("Unhandled exception in import process")
    log("%s" % err)
    sys.exit(1)
