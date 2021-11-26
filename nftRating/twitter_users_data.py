from time import strftime, strptime

from bigQuery import *
from twython import Twython

t = Twython(app_key='CV0IDw8fZ9k0jRKmzfEbKoESS',
            app_secret='yDjfZ39VLTjWrOVmrbZcu0N5L85vzOZPg6fuyJbMnctfKF997V',
            )

twitter_users_data_list = []
twitter_data_list = []
users_lst = get_twitter_users()
users_count = get_rows_count(raw_data_table_id)
i = 0
for row in users_lst:
    i += 1
    twitter_users_data = {}
    print("{}".format(row.twitter))
    try:
        user_data = (t.show_user(screen_name=str(row.twitter)))
    except Exception as ex:
        print(ex)
        continue
    twitter_users_data['uuid'] = str(row.uuid)
    twitter_users_data['twitter_user_id'] = user_data['id']
    twitter_users_data['twitter_followers_count'] = user_data['followers_count']
    twitter_users_data['twitter_friends_count'] = user_data['friends_count']
    twitter_users_data['twitter_favourites_count'] = user_data['favourites_count']
    twitter_users_data['twitter_statuses_count'] = user_data['statuses_count']
    try:
        twitter_users_data['twitter_reg_date'] = strftime('%Y-%m-%d', strptime(user_data['status']['created_at'],
                                                                               '%a %b %d %H:%M:%S +0000 %Y'))
    except Exception as ex:
        print(ex)
        twitter_users_data['twitter_reg_date'] = '0001-01-01'

    twitter_data_list.append(twitter_users_data)
    # bigQuery.client.update_table()
    print("Get data for {} user from {}.".format(i, users_count))

insert_rows_from_json(twitter_users_data_table_id, twitter_data_list)

    # TODO - Find other way to update existing rows, because update each row separately takes to long.
    #  Possible solution - to delete all rows and to insert_rows_from_json.
    # query = """
    # UPDATE {}
    # SET twitter_user_id = {}
    # , twitter_followers_count = {}
    # , twitter_friends_count = {}
    # , twitter_favourites_count = {}
    # , twitter_statuses_count = {}
    # , twitter_reg_date = '{}'
    # WHERE uuid = '{}'
    # """.format(bigQuery.raw_data_table_id, twitter_users_data['twitter_user_id'],
    #            twitter_users_data['twitter_followers_count'], twitter_users_data['twitter_friends_count'],
    #            twitter_users_data['twitter_favourites_count'],
    #            twitter_users_data['twitter_statuses_count'], twitter_users_data['twitter_reg_date'], str(row.uuid))
    # # print(bigQuery.client.get_table(raw_data_table_id).streaming_buffer.oldest_entry_time)
    # query_job = client.query(query)
    # query_job.result()
    # print("DML query modified {} row from {}.".format(i, users_count))users_count
