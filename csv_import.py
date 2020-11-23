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
import urllib.request

# Place where google storage files are copied to
STAGING_DIR = "/tmp/staging"
neo4j_path = os.environ.get('NEO4J_HOME', '/var/lib/neo4j')

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
    if not os.path.exists(STAGING_DIR):
        mkdir(STAGING_DIR)

    result = run_command("gsutil -m cp -r \"%s\" \"%s\"" % (import_from, STAGING_DIR))
    log("Finished copying with %s" % result)

    # Suppose we've copied gs://mybucket/mydataset to the staging dir
    # the available file list is now just "mydataset", or should be.
    available = listdir(STAGING_DIR)
    log("Files available are %s" % listdir(STAGING_DIR))

    if len(available) == 1:
        return join(STAGING_DIR, available[0])

    raise Exception("Unexpected directory contents in %s after copy; did you specify the dataset location correctly?" % STAGING_DIR)

def catcher(fn, message):
    try:
        result = fn()
        log("%s: %s" % (message, result))
        return result
    except Exception as err:
        log("Failed to %s: %s" % (message, err))

def run_command(cmd):
    log("COMMAND: %s" % cmd)
    return subprocess.run(cmd, shell=True, stdout=sys.stdout, stderr=sys.stderr)

def start_neo4j():
    log("Starting Neo4j system service")
    return catcher(lambda: run_command("systemctl start neo4j"), "start neo4j system service")

def stop_neo4j():
    log("Stopping Neo4j system service")
    return catcher(lambda: run_command('systemctl stop neo4j'), "stop neo4j system service")

def destroy_database(db='neo4j'):
    log("Destroying in place 'neo4j' database, to be replaced by import")

    commands = [
        { "cmd": "rm -rf \"%s/data/\"" % neo4j_path, "message": "destroy database state" }
    ]
    
    for command in commands: 
        catcher(lambda: run_command(command['cmd']), command['message'])

    return True

def assign_permissions():
    log("Assigning neo4j:neo4j permissions to newly created database")
    return catcher(
        lambda: run_command("chown -R neo4j:neo4j /var/lib/neo4j/data"), 
        "assign neo4j:neo4j privileges to restored database")

def main(storage = "gs://meetup-data/chicago_crime_bigquery"):
    """Takes an argument of where the CSV import set is located (a directory)"""
    log("Beginning csv_import from %s" % storage)
    stop_neo4j()
    destroy_database('neo4j')
    dir = copy_data_from_storage(storage)

    data_dirs = [f for f in listdir(dir) if isdir(join(dir, f))]
    log(data_dirs)

    nodes = list(filter(lambda e: e.startswith("node_"), data_dirs))
    rels = list(filter(lambda e: e.startswith("rel_"), data_dirs))
    log("Nodes: %s" % nodes)
    log("Relationships: %s" % rels)

    if len(nodes) == 0:
        raise Exception("No valid node CSV directories could be found; there is nothing to import!")

    # Iteratively build the command that will be executed to do import
    import_args = ["neo4j-admin", "import"]

    for node_dir in nodes:
        label = node_dir.replace("node_", "")
        import_args.append(prepare_node(label, join(dir, node_dir)))

    for rel_dir in rels:
        rel_type = rel_dir.replace("rel_", "")
        import_args.append(prepare_rel(rel_type, join(dir, rel_dir)))

    command=" ".join(import_args)
    log("Running command %s" % command)
    result = subprocess.run(import_args, stderr=subprocess.STDOUT)
    log("Finished neo4j-admin import; next assigning permissions")
    assign_permissions()
    log("Re-starting neo4j system service with the new database")
    start_neo4j()

def get_import_set():
    """Attempts to determine from the system environment what import set should be used.

    - First checks neo4j_import_set metadata on the VM
    - Second checks the NEO4J_IMPORT_SET environment variable

    Fails if neither are specified.
    """
    try:
        link = "http://metadata.google.internal/computeMetadata/v1/instance/attributes/neo4j_import_set"
        req = urllib.request.Request(link, None, { "Metadata-Flavor": "Google" })
        import_set = urllib.request.urlopen(req).read().decode("utf-8")
        return import_set
    except Exception as e:
        log("Failed to look up metadata entry 'neo4j_import_set'.  Did you remember to put it on the VM? %s" % e)
        log("Looking for environment variable NEO4J_IMPORT_SET")
        return os.environ['NEO4J_IMPORT_SET']

if __name__ == "__main__":    
    try:
        main(get_import_set())
        sys.exit(0)
    except Exception as err:
        log("Unhandled exception in import process")
        log("%s" % err)
        sys.exit(1)
