import datetime
from time import strftime, strptime
from bigQuery import *
from twython import Twython

# Twitter developer API credentials
t = Twython(app_key='CV0IDw8fZ9k0jRKmzfEbKoESS',
            app_secret='yDjfZ39VLTjWrOVmrbZcu0N5L85vzOZPg6fuyJbMnctfKF997V',
            )


def get_days_from_date(date):
    """
    Function to get days from specific date till today
    :param date: from what date to calculate days
    :return: days as integer
    """
    f_date = datetime.datetime.strptime(date, "%Y-%m-%d")
    l_date = datetime.datetime.strptime(str(datetime.date.today()), "%Y-%m-%d")
    delta = l_date - f_date
    return delta.days


twitter_users_data_list = []
twitter_data_list = []

# Get all twitter users from BigQuery
users_lst = get_twitter_users()
users_count = get_rows_count(raw_data_table_id)
i = 0

# Get user data from twitter, parse and filter it and get specific keys
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
    twitter_users_data['days_from_reg'] = get_days_from_date(twitter_users_data['twitter_reg_date'])
    twitter_data_list.append(twitter_users_data)
    # bigQuery.client.update_table()
    print("Get data for {} user from {}.".format(i, users_count))

# Insert rows into twitter_users_data table in BigQuery
insert_rows_from_json(twitter_users_data_table_id, twitter_data_list)
