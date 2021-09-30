import demoji, re, datetime
import preprocessor


# demoji.download_codes()


class TWPreprocessor:
    @classmethod
    def preprocess(cls, tweet, country, mode=None):
        '''
        Do tweet pre-processing before indexing, make sure all the field data types are in the format as asked in the project doc.
        :param tweet:
        :return: dict
        '''
        # print(tweet.get('text'))
        # print(_text_cleaner(tweet.get('text')))
        # print(_text_cleaner(tweet.get('text'))[0])
        # text,emoji = _text_cleaner(tweet.get('text'))

        if mode == 'POI':
            data = {
                'id': tweet.get('id_str'),
                'poi_name': tweet.get('user').get('screen_name'),
                'poi_id': tweet.get('user').get('id_str'),
                'verified': tweet.get('user').get('verified'),
                'country': country,
                'tweet_text': tweet.get('full_text'),
                'tweet_lang': tweet.get('lang'),
                'text_' + tweet.get('lang'): _text_cleaner(tweet.get('full_text'))[0],
                'hashtags': _get_entities(tweet, 'hashtags'),
                'mentions': _get_entities(tweet, 'mentions'),
                'tweet_urls': _get_entities(tweet, 'urls'),
                'tweet_emoticons': _text_cleaner(tweet.get('full_text'))[1],
                'tweet_date': _get_tweet_date(tweet.get('created_at'))
            }
            if tweet.get('geo') is not None:
                data['geolocation'] = tweet.get('geo').get('coordinates')
            return data
        if mode == 'keywords':
            data = {
                'id': tweet.get('id_str'),
                'verified': False,
                'country': country,
                'tweet_text': tweet.get('full_text'),
                'tweet_lang': tweet.get('lang'),
                'text_' + tweet.get('lang'): _text_cleaner(tweet.get('full_text'))[0],
                'hashtags': _get_entities(tweet, 'hashtags'),
                'mentions': _get_entities(tweet, 'mentions'),
                'tweet_urls': _get_entities(tweet, 'urls'),
                'tweet_emoticons': _text_cleaner(tweet.get('full_text'))[1],
                'tweet_date': _get_tweet_date(tweet.get('created_at'))
            }
            if tweet.get('geo') is not None:
                data['geolocation'] = tweet.get('geo').get('coordinates')
            return data

        # raise NotImplementedError
    @classmethod
    def preprocess1(cls, tweet, country, IdAndTextMap, mode=None):
        if mode == 'reply_mode':
            data = {
                'id': tweet.get('id_str'),
                'verified': False,
                'country': country,
                'replied_to_tweet_id': tweet.get('in_reply_to_status_id'),
                'replied_to_user_id': tweet.get('in_reply_to_user_id'),
                'reply_text': tweet.get('full_text'),
                'tweet_text': IdAndTextMap.get(tweet.get('in_reply_to_status_id_str')),
                'tweet_lang': tweet.get('lang'),
                'text_' + tweet.get('lang'): _text_cleaner(tweet.get('full_text'))[0],
                'mentions': _get_entities(tweet, 'mentions')
            }
            return data


def _get_entities(tweet, type=None):
    result = []
    if type == 'hashtags':
        hashtags = tweet['entities']['hashtags']

        for hashtag in hashtags:
            result.append(hashtag['text'])
    elif type == 'mentions':
        mentions = tweet['entities']['user_mentions']

        for mention in mentions:
            result.append(mention['screen_name'])
    elif type == 'urls':
        urls = tweet['entities']['urls']

        for url in urls:
            result.append(url['url'])

    return result


def _text_cleaner(text):
    emoticons_happy = list([
        ':-)', ':)', ';)', ':o)', ':]', ':3', ':c)', ':>', '=]', '8)', '=)', ':}',
        ':^)', ':-D', ':D', '8-D', '8D', 'x-D', 'xD', 'X-D', 'XD', '=-D', '=D',
        '=-3', '=3', ':-))', ":'-)", ":')", ':*', ':^*', '>:P', ':-P', ':P', 'X-P',
        'x-p', 'xp', 'XP', ':-p', ':p', '=p', ':-b', ':b', '>:)', '>;)', '>:-)',
        '<3'
    ])
    emoticons_sad = list([
        ':L', ':-/', '>:/', ':S', '>:[', ':@', ':-(', ':[', ':-||', '=L', ':<',
        ':-[', ':-<', '=\\', '=/', '>:(', ':(', '>.<', ":'-(", ":'(", ':\\', ':-c',
        ':c', ':{', '>:\\', ';('
    ])
    all_emoticons = emoticons_happy + emoticons_sad

    emojis = list(demoji.findall(text).keys())
    clean_text = demoji.replace(text, '')

    for emo in all_emoticons:
        if (emo in clean_text):
            clean_text = clean_text.replace(emo, '')
            emojis.append(emo)

    clean_text = preprocessor.clean(text)
    # preprocessor.set_options(preprocessor.OPT.EMOJI, preprocessor.OPT.SMILEY)
    # emojis= preprocessor.parse(text)

    return clean_text, emojis


def _get_tweet_date(tweet_date):
    return _hour_rounder(datetime.datetime.strptime(tweet_date, '%a %b %d %H:%M:%S +0000 %Y')).strftime(
        '%Y-%m-%dT%H:%M:%SZ')


def _hour_rounder(t):
    # Rounds to nearest hour by adding a timedelta hour if minute >= 30
    return (t.replace(second=0, microsecond=0, minute=0, hour=t.hour)
            + datetime.timedelta(hours=t.minute // 30))
