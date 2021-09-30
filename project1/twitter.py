import tweepy


class Twitter:
    def __init__(self):
        self.auth = tweepy.OAuthHandler("", "")
        self.auth.set_access_token("", "")
        self.api = tweepy.API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    def _meet_basic_tweet_requirements(self):
        '''
        Add basic tweet requirements logic, like language, country, covid type etc.
        :return: boolean
        '''
        raise NotImplementedError

    def get_tweets_by_lang_country(self, country, keyword):

        raise NotImplementedError

    def get_tweets_by_poi_screen_name(self, poiName, tweetCount):
        '''
        Use user_timeline api to fetch POI related tweets, some postprocessing may be required.
        :return: List
        '''
        # print(self)
        # print(poiName)
        # print(tweetCount)
        _list = []
        for tweet in tweepy.Cursor(self.api.user_timeline, poiName, tweet_mode='extended').items(tweetCount):
            _list.append(tweet)
        return _list
        # poiTweets = self.api.user_timeline(poiName, count=1000)
        # for tweet in poiTweets:
        #     _list.append(tweet)

        # print(_list)
        # print("******************************")
        # poiTweetlist = list(poiTweets)
        # for tweet in poiTweets:
        #    print(type(tweet))
        #   print(tweet,end="\n\n")
        # poiTweetlist.extend(tweet)

        # raise NotImplementedError

    def get_tweets_by_lang_and_keyword(self, count, keyword_name, lang):
        '''
        Use search api to fetch keywords and language related tweets, use tweepy Cursor.
        :return: List
        '''
        _list = []
        for tweets in tweepy.Cursor(self.api.search, q=keyword_name + " -filter:retweets", lang=lang,
                                    tweet_mode='extended').items(count):
            _list.append(tweets)
        return _list
        # raise NotImplementedError

    def get_replies_for_keyword(self,keyword_name,count,IdList):
        '''
        Get replies for a particular tweet_id, use max_id and since_id.
        For more info: https://developer.twitter.com/en/docs/twitter-api/v1/tweets/timelines/guides/working-with-timelines
        :return: List
        '''
        _tweet_keyword_reply = []
        query = keyword_name + ' filter:replies'
        for tweets in tweepy.Cursor(self.api.search, q=query, sinceId=None,
                                    tweet_mode='extended').items(count):
            if hasattr(tweets, 'in_reply_to_status_id_str'):
                if tweets._json.get("in_reply_to_status_id_str") in IdList:
                    print("Got It !!!!!!!!! - " + tweets._json.get("in_reply_to_status_id_str"))
                    _tweet_keyword_reply.append(tweets)
        print("FOUND REPLY FOR KEYWORD -" + keyword_name + "LENGTH" + str(len(_tweet_keyword_reply)))
        return _tweet_keyword_reply
        #raise NotImplementedError
