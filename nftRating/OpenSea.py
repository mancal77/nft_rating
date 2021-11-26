import uuid
import requests
from bigQuery import *

offset = 0
batch_size = 50
max_offset = 100
url = "https://api.opensea.io/api/v1/assets?order_by=sale_date&order_direction=desc&offset={offset}&limit={batch_size}"

filtered = []

while len(filtered) < max_offset:
    print(url.format(offset=offset, batch_size=batch_size))
    response = requests.request("GET", url.format(offset=offset, batch_size=batch_size))
    response_json = response.json()["assets"]
    for k in response_json:
        fil = {}
        res = None
        fil = {key: k[key] for key in k.keys() & {'name', 'description', 'permalink', 'collection', 'asset_contract'}}
        res = fil['collection']['twitter_username']
        fil['twitter'] = res
        del fil['collection']
        res = fil['asset_contract']['address']
        fil['item_token'] = res
        res = fil['asset_contract']['created_date']
        fil['project_reg_date'] = str(res).split('T')[0]
        del fil['asset_contract']
        # TODO possible to add check what item_type and add it
        if fil['twitter'] and fil['name']:
        # TODO - check if it possible to filter out by "created_date". The problem that most of assets sre from 2020.
        # if fil['twitter'] and fil['name'] and (datetime.now() - datetime.strptime(fil['project_reg_date'], '%Y-%m-%d')).days < 120:
            filtered.append(fil)

for i in filtered:
    i['item_type'] = 2
    i['item_name'] = i['name']
    del i['name']
    i['project_url'] = i['permalink']
    del i['permalink']
    i['uuid'] = str(uuid.uuid4())

# TODO - investigate why first run (with create table) fails. Not happens at rarityUpcoming.py run.
insert_rows_from_json(raw_data_table_id, filtered)
