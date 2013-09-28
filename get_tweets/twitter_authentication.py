"""Generate OAuth tokens for the app."""

import argparse
import json

from twython import Twython

class TwitterAuthentication:
    """Handles Twitter authentication of the app and user."""
    
    CONSUMER_CREDENTIALS_FILE = '.consumer_credentials'
    USER_CREDENTIALS_FILE = '.user_credentials'
    
    def __init__(self):
        """Loads app and user credentials from the filesystem."""
        
        # Consumer credentials are stored in a manually edited JSON file and are
        # already expected to exist
        try:
            with open(self.CONSUMER_CREDENTIALS_FILE, 'r') as f:
                self.consumer_credentials = json.load(f)
            print('Consumer credentials loaded.')
        except FileNotFoundError:
            print('Consumer credentials file not found in ' \
                  '\'{0}\'.'.format(self.CONSUMER_CREDENTIALS_FILE))
            return
            
        # User OAuth token and token secret are stored in an automatically 
        # created JSON file. If it doesn't exist they can be generated and saved 
        # there.
        try:
            with open(self.USER_CREDENTIALS_FILE, 'r') as f:
                self.user_credentials = json.load(f)
            print('User credentials loaded.')
        except FileNotFoundError:
            twitter = Twython(self.consumer_credentials['consumer_key'], 
                              self.consumer_credentials['consumer_secret'])
            with open(self.USER_CREDENTIALS_FILE, 'w') as f:
                json.dump(twitter.get_authentication_tokens(), f)
            print('User credentials generated.')
            
    def get_credentials(self):
        """
        Returns a tuple consisting of: APP_KEY, APP_SECRET, OAUTH_TOKEN, 
        OAUTH_TOKEN_SECRET.
        """
        
        return (self.consumer_credentials['consumer_key'],
                self.consumer_credentials['consumer_secret'],
                self.user_credentials['oauth_token'],
                self.user_credentials['oauth_token_secret'])

if __name__ == "__main__":
    ta = TwitterAuthentication()
