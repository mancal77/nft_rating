from twython import Twython

t = Twython(app_key='CV0IDw8fZ9k0jRKmzfEbKoESS',
            app_secret='yDjfZ39VLTjWrOVmrbZcu0N5L85vzOZPg6fuyJbMnctfKF997V',
            # oauth_token=oauth_token,
            # oauth_token_secret=oauth_token_secret
            )

print(t.show_user(screen_name='ShedEVERstudio'))
