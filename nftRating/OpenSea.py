import requests
import os
from pyspark.sql import SparkSession
import pandas as pd
import json

from bigQuery import insert_rows_from_json

spark = SparkSession.builder.getOrCreate()
tmp_file_path = os.path.join(os.getcwd(), 'tmp.json')
offset = 0
batch_size = 50
read_more = True
max_offset = 100
url = "https://api.opensea.io/api/v1/assets?order_direction=desc&offset={offset}&limit={batch_size}"

filtered = []

while len(filtered) < max_offset:
    print(url.format(offset=offset, batch_size=batch_size))
    response = requests.request("GET", url.format(offset=offset, batch_size=batch_size))
    response_json = response.json()["assets"]
    for k in response_json:
        fil = {}
        fil = {key: k[key] for key in k.keys() & {'name', 'description', 'permalink', 'collection', 'asset_contract'}}
        res = fil['collection']['twitter_username']
        del fil['collection']
        fil['twitter'] = res
        res = fil['asset_contract']['address']
        del fil['asset_contract']
        fil['item_token'] = res
        if fil['twitter'] and fil['name']:
            filtered.append(fil)

for i in filtered:
    i['item_type'] = 1
    i['item_name'] = i['name']
    del i['name']
    i['project_url'] = i['permalink']
    del i['permalink']

insert_rows_from_json(filtered)
