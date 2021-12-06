from bigQuery import *
from twython import Twython
from sentiment import *
from time import strftime, strptime

# Twitter developer API credentials
t = Twython(app_key='CV0IDw8fZ9k0jRKmzfEbKoESS',
            app_secret='yDjfZ39VLTjWrOVmrbZcu0N5L85vzOZPg6fuyJbMnctfKF997V',
            )

twits_full_list = []

# Get all twitter user IDs from BigQuery
users_lst = get_twitter_users_id()
users_count = get_rows_count(raw_data_table_id)
i = 0

# Get statuses data from twitter, parse and filter it and get specific keys
for row in users_lst:
    i += 1
    print("{}".format(row.twitter))

    try:
        twits_data = t.search(q=row.twitter)
        twits_data_lst = twits_data['statuses']
    except Exception as ex:
        print(ex)
        twits_data_lst = []

    for twit_payload in twits_data_lst:
        twit_full = {}
        twit = twit_payload['text']
        try:
            twit_sentiment = analyze_sentiment(twit_payload['text'])
        except Exception as ex:
            print(ex)
            twit_sentiment = 0
        try:
            twit_creation_date = strftime('%Y-%m-%d', strptime(twit_payload['created_at'], '%a %b %d %H:%M:%S +0000 %Y'))
        except Exception as ex:
            print(ex)
            twit_creation_date = '0001-01-01'

        twit_full.update({"uuid": row.uuid, "twitter_user_id": row.twitter_user_id, "twit_text": twit,
                          "twit_creation_date": twit_creation_date, "twit_sentiment": twit_sentiment})
        twits_full_list.append(twit_full)
    print("{} user processed from {}.".format(i, users_count))

# Insert rows into twits table in BigQuery
insert_rows_from_json(twits_table_id, twits_full_list)

