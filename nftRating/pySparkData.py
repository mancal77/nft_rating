#!/usr/bin/python
import dictionary as dictionary
from pyspark.sql import SparkSession
from datetime import timedelta, datetime
from bigQuery import *


def query(view):
    """
    the query
    """
    return f"""
            SELECT
                {view} as view
            FROM projects
            """


# os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /venv/lib/python3.9/site-packages/pyspark/jars/spark-sql_2.12-3.2.0.jar'
# os.environ['HADOOP_HOME'] = '~/Downloads/hadoop-2.7.3'
# os.environ['HADOOP_CONF_DIR'] = '$HADOOP_HOME%/etc/hadoop'
# os.environ['LD_LIBRARY_PATH'] = '$HADOOP_HOME/lib/native'
# print(os.system('pwd'))


spark = SparkSession.builder \
  .master('local') \
  .appName('spark-bigquery') \
  .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.2') \
  .getOrCreate()

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
bucket = "dataproc-staging-us-central1-538623213906-fessipzh"
spark.conf.set('temporaryGcsBucket', bucket)

# Load fact_hits from BigQuery.
# projects = spark.read.format('com.google.cloud.spark.bigquery') \
projects = spark.read.format('bigquery') \
    .option('table', raw_data_table_id) \
    .option('select', 'uuid,item_name') \
    .load()
projects.createOrReplaceTempView('projects')
projects.show()

# Loop on instruments query to get all aggr (each aggr will be add seperately to BQ target table
# for k, v in dictionary.items():
#     view = int(k)
#
#     query = query(view=view)
#
#     # Perform query process.
#     query = spark.sql(query)
#     query.show()
#     query.printSchema()
