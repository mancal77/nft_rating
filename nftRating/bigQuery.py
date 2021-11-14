from google.cloud.exceptions import NotFound
from google.cloud import bigquery

client = bigquery.Client()
client.project = 'disco-ascent-328216'
dataset_id = "{}.nft_rating".format(client.project)
table_id = '{}.nft_raw_data'.format(dataset_id)


def create_data_set():
    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)
    # Geographic location where the dataset should reside.
    dataset.location = "US"
    # Send the dataset to the API for creation, with an explicit timeout.
    dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))


def create_table():
    schema = [
        bigquery.SchemaField("item_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("item_type", "STRING", mode="REQUIRED"),  # 1-project, 2-artwork, 3-collection, 4-creator
        bigquery.SchemaField("item_token", "STRING"),
        bigquery.SchemaField("project_description", "STRING"),
        bigquery.SchemaField("twitter", "STRING"),
        bigquery.SchemaField("project_url", "STRING"),
        bigquery.SchemaField("opensea_url", "STRING"),
        bigquery.SchemaField("discord", "STRING"),
        bigquery.SchemaField("project_reg_date", "DATE"),
        bigquery.SchemaField("twitter_reg_date", "DATE"),
        bigquery.SchemaField("new_twits", "INTEGER"),
        bigquery.SchemaField("twitter_followers", "INTEGER"),
        bigquery.SchemaField("twitter_popular_followers", "INTEGER"),
        bigquery.SchemaField("twitter_mentions", "INTEGER"),
        bigquery.SchemaField("additional_functionality", "STRING")
    ]

    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


def insert_rows_from_json(rows_to_insert):
    errors = client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
    if not errors:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))


def insert_rows_from_df(df_to_insert, columns):
    errors = client.insert_rows_from_dataframe(table_id, df_to_insert, columns, 500)  # Make an API request.
    if not errors:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))


# Check if Data Set exists and if not create it
try:
    client.get_dataset(dataset_id)  # Make an API request.
    print("Dataset {} already exists".format(dataset_id))
except NotFound:
    print("Dataset {} is not found".format(dataset_id))
    print("Creating dataset {}".format(dataset_id))
    create_data_set()

# Check if Table exists and if not create it
try:
    client.get_table(table_id)  # Make an API request.
    print("Table {} already exists.".format(table_id))
except NotFound:
    print("Table {} is not found.".format(table_id))
    print("Creating table {}".format(table_id))
    create_table()
