from datetime import date

from pyspark.sql import SparkSession
from bigQuery import *


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
raw_data.createOrReplaceTempView('raw_data')
raw_data.select('uuid').show()

# Load twits from BigQuery.
twits_data = spark.read.format('bigquery') \
    .option('table', twits_table_id) \
    .load()
twits_data.createOrReplaceTempView('twits_data')
twits_data.show()

# Load statuses from BigQuery.
statuses_data = spark.read.format('bigquery') \
    .option('table', twitter_statuses_table_id) \
    .load()
statuses_data.createOrReplaceTempView('statuses_data')


# Load user_data from BigQuery.
user_data = spark.read.format('bigquery') \
    .option('table', raw_data_table_id) \
    .load()
user_data.createOrReplaceTempView('user_data')


# Query Build
def query1(uuid):
    """
    the query
    """
    return f"""
            SELECT
                user_data.twitter_reg_date,
                user_data.twitter_followers_count,
                user_data.twitter_statuses_count,
                user_data.uuid,
                avg(twits_data.twit_sentiment) as avg_sentiment,
                count(twits_data.*) as total_twits
            FROM user_data
            JOIN twits_data on user_data.uuid=twits_data.uuid
            WHERE user_data.uuid == {uuid}
            group by 1,2,3,4
            """


def query2(uuid):
    return f"""
                SELECT
                    count(twits_data.*) as total_twits_7
                FROM twits_data 
                WHERE twits_data.twit_creation_date>=DATEADD(DAY,-7,GETDATE())
                AND twits.data.uuid = {uuid}
                """

# Loop on projects view to calculate rating and to send data to MySQL
for row in raw_data.collect():
    row['uuid']
    a = spark.sql(query1(row['uuid']))
    a.show()
    print(a)

    # twits_7count = twits_data.groupby(row['uuid']).count('uuid').filter((date.today() - twits_data.select('twit_creation_date')).days < 7)
    # print(twits_7count)
