from datetime import date


def calculate_rating(twitter_reg_date, followers, statuses_since_reg, avg_twits_sentiment, total_tweets, total_tweets_7days):
    days_since_reg = (date.today() - twitter_reg_date).days
    if days_since_reg == 0:
        days_since_reg = 1

    return followers * 0.2 / days_since_reg + statuses_since_reg * 0.2 / days_since_reg + \
           avg_twits_sentiment * 0.3 + total_tweets * 0.1 / days_since_reg + \
           total_tweets_7days * 0.2
