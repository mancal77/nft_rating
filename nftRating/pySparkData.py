from pyspark.sql import SparkSession
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


spark = SparkSession.builder \
  .master('local') \
  .appName('spark-bigquery') \
  .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.2') \
  .getOrCreate()


# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
# bucket = "dataproc-staging-us-central1-538623213906-fessipzh"
# spark.conf.set('temporaryGcsBucket', bucket)


# Load raw_data from BigQuery.
raw_data = spark.read.format('bigquery') \
    .option('table', raw_data_table_id) \
    .load()
raw_data.createOrReplaceTempView('projects')


# Load twits from BigQuery.
twits_data = spark.read.format('bigquery') \
    .option('table', twits_table_id) \
    .load()
twits_data.createOrReplaceTempView('projects')


# Load user_data from BigQuery.
user_data = spark.read.format('bigquery') \
    .option('table', raw_data_table_id) \
    .load()
user_data.createOrReplaceTempView('projects')
user_data.show()

# Load statuses from BigQuery.
statuses_data = spark.read.format('bigquery') \
    .option('table', twitter_statuses_table_id) \
    .load()
statuses_data.createOrReplaceTempView('projects')


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
