import bigQuery
from bigQuery import *
from twython import Twython

t = Twython(app_key='CV0IDw8fZ9k0jRKmzfEbKoESS',
            app_secret='yDjfZ39VLTjWrOVmrbZcu0N5L85vzOZPg6fuyJbMnctfKF997V',
            )

twits_full_list = []
users_lst = get_twitter_users_id()
users_count = bigQuery.client.get_table(raw_data_table_id).num_rows
i = 0
for row in users_lst:
    i += 1
    print("{}".format(row.twitter))

    twits_data = t.search(q=row.twitter)
    twits_data_lst = twits_data['statuses']
    for twit_payload in twits_data_lst:
        twit_full = {}
        twit = twit_payload['text']
        twit_full.update({"uuid": row.uuid, "twitter_user_id": row.twitter_user_id, "twit_text": twit})
        twits_full_list.append(twit_full)
    print("{} user processed from {}.".format(i, users_count))

insert_rows_from_json(twits_table_id, twits_full_list)

