from pyspark.sql.functions import col, datediff, current_date
from pyspark.sql import SparkSession, functions
from bigQuery import *


spark = SparkSession.builder \
    .master('local') \
    .appName('spark-bigquery') \
    .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.2') \
    .getOrCreate()

# Use the Cloud Storage bucket for temporary BigQuery export data used - NO NEED TO USE DATAPROC
# by the connector.
# bucket = "dataproc-staging-us-central1-538623213906-fessipzh"
# spark.conf.set('temporaryGcsBucket', bucket)


# Load raw_data from BigQuery.
raw_data = spark.read.format('bigquery') \
    .option('table', raw_data_table_id) \
    .load()
raw_data.createOrReplaceTempView('raw_data')
raw_data.show()

spark.conf.set("viewsEnabled", "true")
spark.conf.set("materializationDataset", 'nft_rating')

# Load twits from BigQuery.
sql = """SELECT * FROM {} LIMIT 1000""".format(twits_table_id)
twits_data = spark.read.format("bigquery").load(sql)
twits_data.show()

# # Load statuses from BigQuery.
# statuses_data = spark.read.format('bigquery') \
#     .option('table', twitter_statuses_table_id) \
#     .load()
# statuses_data.createOrReplaceTempView('statuses_data')
# statuses_data.show()

# Load user_data from BigQuery.
user_data = spark.read.format('bigquery') \
    .option('table', twitter_users_data_table_id) \
    .load()
user_data.createOrReplaceTempView('user_data')
user_data.show()

# Loop on projects view to calculate rating and to send data to MySQL
for row in raw_data.collect():
    twitter_reg_date = user_data.where(col('uuid') == row['uuid']).select('twitter_reg_date')
    followers = user_data.where(col('uuid') == row['uuid']).select('twitter_followers_count')
    statuses_since_reg = user_data.where(col('uuid') == row['uuid']).agg(functions.sum(col('twitter_statuses_count')))
    avg_twits_sentiment = twits_data.where(col('uuid') == row['uuid']).agg(functions.avg(col('twit_sentiment')))
    total_tweets = twits_data.where(col('uuid') == row['uuid']).agg(functions.count(col('uuid')))
    total_7day_tweets = twits_data.where((col('uuid') == row['uuid']) & ((datediff(current_date(), col("twit_creation_date"))) < 7)).count()
    # transfer to kafka
