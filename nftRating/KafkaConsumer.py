from kafka import KafkaConsumer
import json
from datetime import datetime
import re
import pyarrow as pa
import mysql.connector as mc

# MySQL settings
mysql_port = 3306
mysql_database_name = 'NFT'
mysql_table_name = 'ratings'
mysql_username = 'naya'
mysql_password = 'NayaPass1!'
host = 'localhost'

# Kafka settings
topic_name = 'kafka-nft'
brokers = ['cnt7-naya-cdh63:9092']

# Set kafka consumer
consumer = KafkaConsumer(
    topic_name,
    group_id='File_MySQL_HDFS',
    bootstrap_servers=brokers,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    auto_commit_interval_ms=1000)

insert_statement = """
    INSERT INTO NFT.ratings(uuid, item_name, twitter, url, rating) 
    VALUES ('{}', '{}', '{}', '{}', {});"""

# connector to mysql
mysql_conn = mc.connect(
    user=mysql_username,
    password=mysql_password,
    host=host,
    port=mysql_port,
    autocommit=True,  # <--
    database=mysql_database_name)


def mysql_event_insert(mysql_con, uuid, item_name, twitter, url, rating):
    mysql_cursor = mysql_con.cursor()
    sql = insert_statement.format(uuid, item_name, twitter, url, rating)
    print(f"SQL = {sql}")
    mysql_cursor.execute(sql)
    mysql_cursor.close()


for message in consumer:
    event = message.value.decode('ascii')
    uuid, item_name, twitter, url, rating = event.split('|')
    # Write to MySQL
    mysql_event_insert(mysql_conn, uuid, item_name, twitter, url, rating)
