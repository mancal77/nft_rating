from time import strftime, strptime

import bigQuery
from bigQuery import *
from twython import Twython

t = Twython(app_key='CV0IDw8fZ9k0jRKmzfEbKoESS',
            app_secret='yDjfZ39VLTjWrOVmrbZcu0N5L85vzOZPg6fuyJbMnctfKF997V',
            )


twitter_data_list = []
proj_lst = get_twitter_users()
for row in proj_lst:
    twitter_data = {}
    print("{}".format(row.twitter))
    try:
        user = (t.show_user(screen_name=str(row.twitter)))
    except Exception as ex:
        print(ex)
    twitter_data['uuid'] = str(row.uuid)
    twitter_data['twitter_followers_count'] = user['followers_count']
    twitter_data['twitter_friends_count'] = user['friends_count']
    twitter_data['twitter_favourites_count'] = user['favourites_count']
    twitter_data['twitter_statuses_count'] = user['statuses_count']
    twitter_data['twitter_reg_date'] = strftime('%Y-%m-%d', strptime(user['status']['created_at'], '%a %b %d %H:%M:%S +0000 %Y'))

    # twitter_data_list.append(twitter_data)
    # bigQuery.client.update_table()
    # insert_rows_from_json(twitter_data_list)

    # TODO - Find other way to update existing rows, because update each row separately takes to long.
    #  Possible solution - to delete all rows and to insert_rows_from_json.
    query = """
    UPDATE {}
    SET twitter_followers_count = {}
    , twitter_friends_count = {}
    , twitter_favourites_count = {}
    , twitter_statuses_count = {}
    , twitter_reg_date = '{}'
    WHERE uuid = '{}'
    """.format(bigQuery.table_id, twitter_data['twitter_followers_count'], twitter_data['twitter_friends_count'], twitter_data['twitter_favourites_count'],
               twitter_data['twitter_statuses_count'], twitter_data['twitter_reg_date'], str(row.uuid))
    query_job = client.query(query)
    query_job.result()
    print(f"DML query modified {query_job.num_dml_affected_rows} rows.")
