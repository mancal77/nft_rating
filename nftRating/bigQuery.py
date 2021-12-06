from google.cloud.exceptions import NotFound
from google.cloud import bigquery


client = bigquery.Client()
client.project = 'disco-ascent-328216'
dataset_id = "{}.nft_rating".format(client.project)
raw_data_table_id = '{}.nft_raw_data'.format(dataset_id)
twitter_users_data_table_id = '{}.twitter_users_data'.format(dataset_id)
twitter_statuses_table_id = '{}.twitter_statuses'.format(dataset_id)
twits_table_id = '{}.twits'.format(dataset_id)


def create_data_set():
    """
    Checks if dataset exists and creates it if not
    :return: Nothing
    """
    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)
    # Geographic location where the dataset should reside.
    dataset.location = "US"
    # Send the dataset to the API for creation, with an explicit timeout.
    dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))


def create_raw_data_table():
    """
    Checks if raw_data table exists and creates it if not.
    Contains table schema.
    :return: Nothing
    """
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
        bigquery.SchemaField("new_twits", "INTEGER"),
        bigquery.SchemaField("twitter_popular_followers", "INTEGER"),
        bigquery.SchemaField("twitter_mentions", "INTEGER"),
        bigquery.SchemaField("additional_functionality", "STRING")
    ]

    table = bigquery.Table(raw_data_table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


def create_twitter_users_data_table():
    """
    Checks if twitter_users_data table exists and creates it if not.
    Contains table schema.
    :return: Nothing
    """
    schema = [
        bigquery.SchemaField("uuid", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("twitter_user_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("twitter_followers_count", "INTEGER"),
        bigquery.SchemaField("twitter_friends_count", "INTEGER"),
        bigquery.SchemaField("twitter_favourites_count", "INTEGER"),
        bigquery.SchemaField("twitter_statuses_count", "INTEGER"),
        bigquery.SchemaField("twitter_reg_date", "DATE"),
        bigquery.SchemaField("days_from_reg", "INTEGER"),
    ]

    table = bigquery.Table(twitter_users_data_table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


def create_twitter_statuses_table():
    """
    Checks if twitter_statuses table exists and creates it if not.
    Contains table schema.
    :return:
    """
    schema = [
        bigquery.SchemaField("uuid", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("twitter_user_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("twitter_status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("status_sentiment", "BIGDECIMAL"),
    ]

    table = bigquery.Table(twitter_statuses_table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


def create_twits_table():
    """
    Checks if twits table exists and creates it if not.
    Contains table schema.
    :return:
    """
    schema = [
        bigquery.SchemaField("uuid", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("twitter_user_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("twit_text", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("twit_creation_date", "DATE"),
        bigquery.SchemaField("twit_sentiment", "BIGDECIMAL"),
    ]

    table = bigquery.Table(twits_table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


def insert_rows_from_json(table, rows_to_insert):
    """
    Inserts rows into table. Uses streaming insert.
    :param table: table name that rows should be inserted into
    :param rows_to_insert: JSON file with rows that should be inserted
    :return: Nothing
    """
    errors = client.insert_rows_json(table, rows_to_insert)  # Make an API request.
    if not errors:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))


def get_twitter_users():
    """
    Get rows from raw_data table.
    :return: List of uuid and twitter columns values from raw_data table.
    """
    query = """
        SELECT uuid, twitter
        FROM {}
    """.format(raw_data_table_id)
    query_job = client.query(query)  # Make an API request.
    rows = query_job.result()
    return rows


def get_twitter_users_id():
    """
    Get rows from raw_data and twitter_users tables.
    JOIN two table by uuid column.
    :return: List of uuid and twitter columns values from raw_data table and twitter_user_id from twitter_users table.
    """
    query = """
        SELECT nrd.uuid, tud.twitter_user_id, nrd.twitter
        FROM {}  AS nrd
        JOIN {}  AS tud ON nrd.uuid = tud.uuid
        GROUP BY
        nrd.uuid, tud.twitter_user_id, nrd.twitter
    """.format(raw_data_table_id, twitter_users_data_table_id)
    query_job = client.query(query)  # Make an API request.
    rows = query_job.result()
    return rows


def get_rows_count(table):
    """
    Get count of rows from specific table
    :param table: Table to count rows.
    :return: Rows quantity
    """
    query = """SELECT COUNT(*) AS count FROM {}""".format(table)
    query_job = client.query(query)  # Make an API request.
    rows = query_job.result()
    for row in rows:
        return row.count


"""
Check if Data Set exists and if not create it
"""
try:
    client.get_dataset(dataset_id)  # Make an API request.
    print("Dataset {} already exists".format(dataset_id))
except NotFound:
    print("Dataset {} is not found".format(dataset_id))
    print("Creating dataset {}".format(dataset_id))
    create_data_set()

"""
Check if raw_data_table exists and if not create it
"""
try:
    client.get_table(raw_data_table_id)  # Make an API request.
    print("Table {} already exists.".format(raw_data_table_id))
except NotFound:
    print("Table {} is not found.".format(raw_data_table_id))
    print("Creating table {}".format(raw_data_table_id))
    create_raw_data_table()

"""
Check if twitter_users_data table exists and if not create it
"""
try:
    client.get_table(twitter_users_data_table_id)  # Make an API request.
    print("Table {} already exists.".format(twitter_users_data_table_id))
except NotFound:
    print("Table {} is not found.".format(twitter_users_data_table_id))
    print("Creating table {}".format(twitter_users_data_table_id))
    create_twitter_users_data_table()

"""
Check if twitter_statuses_table exists and if not create it
"""
try:
    client.get_table(twitter_statuses_table_id)  # Make an API request.
    print("Table {} already exists.".format(twitter_statuses_table_id))
except NotFound:
    print("Table {} is not found.".format(twitter_statuses_table_id))
    print("Creating table {}".format(twitter_statuses_table_id))
    create_twitter_statuses_table()

"""
Check if twits_table exists and if not create it
"""
try:
    client.get_table(twits_table_id)  # Make an API request.
    print("Table {} already exists.".format(twits_table_id))
except NotFound:
    print("Table {} is not found.".format(twits_table_id))
    print("Creating table {}".format(twits_table_id))
    create_twits_table()
