from twython import TwythonStreamer

from twitter_authentication import TwitterAuthentication

class SimpleStreamer(TwythonStreamer):
    """A simple Twitter stream reading class used for debug purposes."""

    def on_success(self, data):
        if 'text' in data:
            print('{id}: {text}'.format(id=data['id'], text=data['text']))

    def on_error(self, status_code, data):
        print(data)
        self.disconnect()

if __name__ == '__main__':
    stream = SimpleStreamer(*TwitterAuthentication().get_credentials())
    stream.statuses.sample(language='en')
