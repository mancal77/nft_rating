from google.cloud.exceptions import NotFound
from google.cloud import bigquery


class bigQuery:
    client = bigquery.Client()
    client.project = 'disco-ascent-328216'
    dataset_id = "{}.nft_rating".format(client.project)
    table_id = '{}.nft_raw_data'.format(dataset_id)

    def __init__(self):
        # self.client = bigquery.Client()
        self.client.project = client.project
        self.dataset_id = dataset_id
        self.table_id = table_id

    def create_data_set(self):
        # Construct a full Dataset object to send to the API.
        dataset = bigquery.Dataset(self.dataset_id)
        # Geographic location where the dataset should reside.
        dataset.location = "US"
        # Send the dataset to the API for creation, with an explicit timeout.
        dataset = self.client.create_dataset(dataset, timeout=30)  # Make an API request.
        print("Created dataset {}.{}".format(self.client.project, dataset.dataset_id))

    def create_table(self):
        schema = [
            bigquery.SchemaField("project_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("project_description", "STRING"),
            bigquery.SchemaField("discord", "STRING"),
            bigquery.SchemaField("twitter", "STRING"),
            bigquery.SchemaField("project_url", "STRING"),
            bigquery.SchemaField("project_reg_date", "DATE"),
            bigquery.SchemaField("twitter_reg_date", "DATE"),
            bigquery.SchemaField("new_twits", "INTEGER"),
            bigquery.SchemaField("twitter_followers", "INTEGER"),
            bigquery.SchemaField("twitter_popular_followers", "INTEGER"),
            bigquery.SchemaField("twitter_mentions", "INTEGER"),
            bigquery.SchemaField("additional_functionality", "STRING")
        ]

        table = bigquery.Table(self.table_id, schema=schema)
        table = self.client.create_table(table)  # Make an API request.
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )

    def insert_rows(self, rows_to_insert):
        errors = self.client.insert_rows_json(self.table_id, rows_to_insert)  # Make an API request.
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
