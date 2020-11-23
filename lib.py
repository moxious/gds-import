import os
from pyspark.sql.utils import AnalysisException
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import col

class Neo4jImportCSV:
  """Helper object that reformats dataframes to be the format needed by Neo4j-Admin Import
  
  Trivial example usage; imagine you have data like this:

  person_id,name,job_id,name
  1,David,5,Programmer
  2,Sarah,6,Programmer

  node_person = myDf.select('person_id', 'name')
  node_job = myDf.select('job_id', 'job_name')
  rel_person_job = myDf.select('person_id', 'job_id')
  
  helper = Neo4jImportCSV(spark, bucket="gs://my-storage", file_storage_path="my-dataset")
  
  # Create CSV for (:Person)
  helper.write_nodes(node_person, 'Person', 'person_id')

  # Create CSV for (:Job)
  helper.write_nodes(node_job, 'Job', 'job_id')

  # Create CSV for (:Person)-[:HAS]->(:Job)
  helper.write_rels(rel_person_job, 'Person', 'person_id', 'HAS', 'Job', 'job_id')
  """

  """Mappings describing how spark types turn into Neo4j recognized types"""
  type_mappings = {
    "string": "String",
    "int": "Int",
    "float": "Float",
    "bigint": "Long",
    "double": "Double"
  }

  def __init__(self, spark, bucket, file_storage_path="mydataset"):
    self.bucket = bucket
    self.spark = spark
    self.file_storage_path = file_storage_path

    if self.bucket is None:
      raise Exception("You must specify a bucket, such as gs://my-data/")

  def rename_df_for_types(self, df, pk, label):
    """This function renames columns according to their proper neo4j type names.  For information on why we
    do this, see: https://neo4j.com/docs/operations-manual/current/tools/import/file-header-format/#import-tool-header-format
    
    This function will drop any duplicates, as well as records where the primary key is null, because these cannot be imported
    """
    
    to_process = df.where(col(pk).isNotNull()).dropDuplicates([pk])
    
    mappings = {}
    for name, dtype in to_process.dtypes:
      if not dtype in Neo4jImportCSV.type_mappings:
        print("Col %s is of type %s which has no type mapping; assuming string" % (name, dtype))

      neo4j_type_name = type_mappings.get(dtype, "string")
      
      if name == pk:
        renamed = "%s:ID(%s)" % (name, label)
        to_process = to_process.withColumnRenamed(name, renamed)
        mappings[name] = renamed
      else:
        renamed = "%s:%s" % (name, neo4j_type_name)
        to_process = to_process.withColumnRenamed(name, renamed)
        mappings[name] = renamed

    print(mappings)
    return { "data": to_process, "mappings": mappings }
    
  def rename_rel_df_for_types(self, df, source_label, source_key, target_label, target_key):
    """This function renames columns of a dataframe according to Neo4j's import rules found here:
      https://neo4j.com/docs/operations-manual/current/tools/import/file-header-format/
      
      As a simple example of a relationship table with schema person_id,job_id this might turn into
      :START_ID(person_id:string),:END_ID(job_id:string)
      
      This function will drop all records which have null IDs on either end.
    """
    to_process = df.where(col(source_key).isNotNull()).where(col(target_key).isNotNull())

    # Do START_ID and END_ID renaming; this requires looking up their mapped type names from the
    # previous step.
    start_id=":START_ID(%s)" % source_label
    end_id=":END_ID(%s)" % target_label
    return to_process.withColumnRenamed(source_key, start_id) \
      .withColumnRenamed(target_key, end_id)

  def save_csv_to_storage(self, df, path):
    # neo4j-admin import does not want headers in each file; needs a header file
    return df.write \
      .format("csv").mode("overwrite") \
      .option("header", "false") \
      .save(os.path.join(self.bucket, path))

  def neo4j_headers(self, df):
    """Takes a dataframe, and creates another dataframe which represents the header file for neo4j-admin import"""
    schema =  StructType([StructField(name, StringType(), True) for name in df.schema.names])
    data = [tuple(df.schema.names)]
    print(schema)
    print(data)
    return self.spark.createDataFrame(data, schema)

  def save_headers(self, df, path):
    """Given a dataframe of headers from neo4j_headers, writes a file to the dataset path under the headers directory
    with a single CSV file corresponding to the headers of the CSV partitions"""
    target=os.path.join(self.bucket, os.path.join(path, "headers"))
    print("Headers for this frame")
    df.show()
    print("Saving headers to %s" % target)
    # Repartition 1 is important because header files only have 1 row, and we want guaranteed 1 CSV file
    return df.repartition(1).write \
      .format("csv").mode("overwrite") \
      .option("header", "false") \
      .save(target)

  def write_nodes(self, df, label, key):
    """Writes nodes in a dataframe to partitioned CSV which can be imported by neo4j-admin import.

    Parameters (all required):
       - df (the dataframe to write)
       - label (the node label for Neo4j)
       - key (name of the key column in the dataframe)
    """
    print("\nwrite_node_batch %s, %s" % (label, key))
    file = os.path.join(self.file_storage_path, "node_%s" % label)
    renamed = self.rename_df_for_types(df, key, label)
    self.save_csv_to_storage(renamed['data'], file)
    self.save_headers(self.neo4j_headers(renamed['data']), file)
    print("Finished writing node batch for %s => %s" % (label, file))
    return file

  def write_rels(self, df, source_label, source_key, rel_type, target_label, target_key):
    """Writes relationships in a dataframe to partitioned CSV, which can be imported by neo4j-admin import.

    For example

    Parameters (all required):
       - df (the dataframe to write)
       - source_label: the label at the beginning of the relationship
       - source_key: key column name for the source label
       - rel_type: the name of the relationship type in Neo4j
       - target_label: the label at the end of the relationship
       - target_key: key column name for the target label
    """
    file = os.path.join(self.file_storage_path, "rel_%s_%s_%s" % (source_label, rel_type, target_label))
    renamed = self.rename_rel_df_for_types(df, source_label, source_key, target_label, target_key)
    self.save_csv_to_storage(renamed, file)
    self.save_headers(self.neo4j_headers(renamed), file)
    print("Finished writing rel batch for %s -[:%s] -> %s to file %s/%s" % (source_label, rel_type, target_label, self.bucket, file))
    return file
