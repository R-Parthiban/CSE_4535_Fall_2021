import json
import datetime
import pandas as pd
from twitter import Twitter
from tweet_preprocessor import TWPreprocessor
from indexer import Indexer

reply_collection_knob = True


def read_config():
    with open("config_org.json") as json_file:
        data = json.load(json_file)

    return data


def read_keywordFile():
    with open("keywordFile.json") as json_file:
        data = json.load(json_file)
    return data


def write_config(data):
    with open("config_org.json", 'w') as json_file:
        json.dump(data, json_file)

def unload_keyword_pickle_file(keyword_id):
    fileName = "keywords_processed_tweets_"+keyword_id+".pkl"
    unpickled_file = pd.read_pickle('processed_tweets/'+fileName)
    '''
        Id list
        '''
    _IdList = list(unpickled_file['id'])
    _fullTextList = unpickled_file['tweet_text']
    _IdAndTextMap = {}
    for i in range(len(_fullTextList)):
        _IdAndTextMap[_IdList[i]] = _fullTextList[i]
    #raise NotImplementedError
    return _IdAndTextMap,_IdList

def save_file(data, filename):
    df = pd.DataFrame(data)
    df.to_pickle("data/" + filename)


def save_file1(data, filename, dirname):
    df = pd.DataFrame(data)
    df.to_pickle(dirname + filename)


def read_file(type, id):
    return pd.read_pickle(f"data/{type}_{id}.pkl")


def main():
    config = read_config()
    indexer = Indexer()
    twitter = Twitter()
    MAX_RETWEET_COUNT = -1
    keywordFile = read_keywordFile()

    covidAndVaccineList = keywordFile['covid']
    covidAndVaccineList.extend(keywordFile['vaccine'])
    # vaccineList = keywordFile['vaccine']
    pois = config["pois"]
    keywords = config["keywords"]

    # raw_tweets = twitter.get_tweets_by_poi_screen_name(pois[0]["screen_name"], pois[0]["count"])
    # print(raw_tweets)
    # print(type(raw_tweets[0]._json))

    for i in range(len(pois)):
        covid_related_tweet_count = 0
        if pois[i]["finished"] == 0:
            print(f"---------- collecting tweets for poi: {pois[i]['screen_name']}")
            raw_tweets = twitter.get_tweets_by_poi_screen_name(pois[i]["screen_name"], pois[i]["count"])

            save_file1(raw_tweets, f"poi_raw_tweets_poi_{pois[i]['id']}.pkl", "raw_extracted_tweets/")

            covid_related_tweet_ids = []
            processed_tweets = []
            for tw in raw_tweets:
                if (tw._json.get("full_text").startswith('RT')) is False:
                    processed_tweets.append(TWPreprocessor.preprocess(tw._json, pois[i]["country"], 'POI'))
                elif MAX_RETWEET_COUNT >= 0:
                    processed_tweets.append(TWPreprocessor.preprocess(tw._json, pois[i]["country"], 'POI'))
                    MAX_RETWEET_COUNT -= 1
                if any(word in tw._json.get("full_text") for word in covidAndVaccineList):
                    covid_related_tweet_ids.append(tw._json.get('id_str'))
                    covid_related_tweet_count += 1

            save_file1(processed_tweets, f"poi_raw_processed_tweets_poi_{pois[i]['id']}.pkl", "processed_tweets/")
            indexer.create_documents(processed_tweets)

            pois[i]["finished"] = 1
            pois[i]["collected"] = len(processed_tweets)
            pois[i]["covid_related_tweet_ids"] = covid_related_tweet_ids
            pois[i]["covid_related_tweet_count"] = covid_related_tweet_count

            write_config({
                "pois": pois, "keywords": keywords
            })

            save_file(processed_tweets, f"poi_{pois[i]['id']}.pkl")
            print(MAX_RETWEET_COUNT)
            print("------------ process complete -----------------------------------")

    for i in range(len(keywords)):
        # if keywords[i]["finished"] == 1:
        #     if keywords[i]["lang"] == 'en':
        #         keywords[i]["reply_collected"] = 0
        #     write_config({
        #         "pois": pois, "keywords": keywords
        #     })
        if keywords[i]["finished"] == 0:
            print(f"---------- collecting tweets for keyword: {keywords[i]['name']}")
            raw_tweets = twitter.get_tweets_by_lang_and_keyword(keywords[i]["count"], keywords[i]["name"],
                                                                keywords[i]["lang"])

            save_file1(raw_tweets, f"processed_tweets_poi_{keywords[i]['id']}.pkl", "raw_extracted_tweets/")

            processed_tweets = []
            for tw in raw_tweets:
                if tw._json.get('verified') is None:
                    if (tw._json.get("full_text").startswith('RT')) is False:
                        processed_tweets.append(TWPreprocessor.preprocess(tw._json, keywords[i]["country"], 'keywords'))
                    elif MAX_RETWEET_COUNT >= 0:
                        processed_tweets.append(TWPreprocessor.preprocess(tw._json, keywords[i]["country"], 'keywords'))
                        MAX_RETWEET_COUNT -= 1

            save_file1(raw_tweets, f"keyword_raw_tweets_poi_{keywords[i]['id']}.pkl", "processed_tweets/")

            indexer.create_documents(processed_tweets)

            keywords[i]["finished"] = 1
            keywords[i]["collected"] = len(processed_tweets)

            write_config({
                "pois": pois, "keywords": keywords
            })

            save_file1(processed_tweets, f"keywords_processed_tweets_{keywords[i]['id']}.pkl", "processed_tweets/")
            print("----------MAX_RETWEET_COUNT-----" + str(MAX_RETWEET_COUNT))
            print("------------ process complete -----------------------------------")

    if reply_collection_knob:
        # Write a driver logic for reply collection, use the tweets from the data files for which the replies are to collected.
        for i in range(len(keywords)):
            if keywords[i]["reply_collected"] == 0:
                print(f"---------- collecting tweets for keyword: {keywords[i]['name']}")
                IdAndTextMap,IdList = unload_keyword_pickle_file(keywords[i]["id"])
                raw_tweets = twitter.get_replies_for_keyword(keywords[i]["name"],keywords[i]["count"],IdList)

                processed_tweets = []
                for tw in raw_tweets:
                    processed_tweets.append(TWPreprocessor.preprocess1(tw._json, keywords[i]["country"],IdAndTextMap,'reply_mode'))

                # print("raw_tweets" + raw_tweets)
                indexer.create_documents(processed_tweets)

                keywords[i]["reply_collected"] = 1
                keywords[i]["reply_collected_count"] = len(processed_tweets)

                write_config({
                    "pois": pois, "keywords": keywords
                })

                save_file1(processed_tweets, f"keywords_replies_processed_tweets_{keywords[i]['id']}.pkl", "processed_tweets/")
                print("------------ process complete -----------------------------------")

        #raise NotImplementedError


if __name__ == "__main__":
    main()
