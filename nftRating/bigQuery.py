from time import sleep

from google.cloud.exceptions import NotFound
from google.cloud import bigquery

client = bigquery.Client()
client.project = 'disco-ascent-328216'
dataset_id = "{}.nft_rating".format(client.project)
raw_data_table_id = '{}.nft_raw_data'.format(dataset_id)
twitter_statuses_table_id = '{}.twitter_statuses'.format(dataset_id)


def create_data_set():
    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)
    # Geographic location where the dataset should reside.
    dataset.location = "US"
    # Send the dataset to the API for creation, with an explicit timeout.
    dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))


def create_raw_data_table():
    schema = [
        bigquery.SchemaField("uuid", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("item_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("item_type", "STRING", mode="REQUIRED"),  # 1-project, 2-artwork, 3-collection, 4-creator
        bigquery.SchemaField("item_token", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("twitter", "STRING"),
        bigquery.SchemaField("project_url", "STRING"),
        bigquery.SchemaField("opensea_url", "STRING"),
        bigquery.SchemaField("discord", "STRING"),
        bigquery.SchemaField("project_reg_date", "DATE"),
        bigquery.SchemaField("twitter_user_id", "INTEGER"),
        bigquery.SchemaField("twitter_reg_date", "DATE"),
        bigquery.SchemaField("new_twits", "INTEGER"),
        bigquery.SchemaField("twitter_followers_count", "INTEGER"),
        bigquery.SchemaField("twitter_friends_count", "INTEGER"),
        bigquery.SchemaField("twitter_favourites_count", "INTEGER"),
        bigquery.SchemaField("twitter_statuses_count", "INTEGER"),
        bigquery.SchemaField("twitter_popular_followers", "INTEGER"),
        bigquery.SchemaField("twitter_mentions", "INTEGER"),
        bigquery.SchemaField("additional_functionality", "STRING")
    ]

    table = bigquery.Table(raw_data_table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


def create_twitter_statuses_table():
    schema = [
        bigquery.SchemaField("uuid", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("twitter_user_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("twitter_status", "STRING", mode="REQUIRED"),
    ]

    table = bigquery.Table(twitter_statuses_table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


def insert_rows_from_json(table, rows_to_insert):
    errors = client.insert_rows_json(table, rows_to_insert)  # Make an API request.
    if not errors:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))


def insert_rows_from_df(df_to_insert, columns):
    errors = client.insert_rows_from_dataframe(raw_data_table_id, df_to_insert, columns, 500)  # Make an API request.
    if not errors:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))


def get_twitter_users():
    query = """
        SELECT uuid, twitter
        FROM {}
    """.format(raw_data_table_id)
    query_job = client.query(query)  # Make an API request.
    rows = query_job.result()
    return rows


def get_twitter_users_id():
    query = """
        SELECT uuid, twitter_user_id
        FROM {}
    """.format(raw_data_table_id)
    query_job = client.query(query)  # Make an API request.
    rows = query_job.result()
    return rows


# Check if Data Set exists and if not create it
try:
    client.get_dataset(dataset_id)  # Make an API request.
    print("Dataset {} already exists".format(dataset_id))
except NotFound:
    print("Dataset {} is not found".format(dataset_id))
    print("Creating dataset {}".format(dataset_id))
    create_data_set()

# Check if raw_data_table exists and if not create it
try:
    client.get_table(raw_data_table_id)  # Make an API request.
    print("Table {} already exists.".format(raw_data_table_id))
except NotFound:
    print("Table {} is not found.".format(raw_data_table_id))
    print("Creating table {}".format(raw_data_table_id))
    create_raw_data_table()

# Check if twitter_statuses_table exists and if not create it
try:
    client.get_table(twitter_statuses_table_id)  # Make an API request.
    print("Table {} already exists.".format(raw_data_table_id))
except NotFound:
    print("Table {} is not found.".format(raw_data_table_id))
    print("Creating table {}".format(raw_data_table_id))
    create_twitter_statuses_table()
