import os
import pysolr
import requests

# CORE_NAME = "IRF21_class_demo"
CORE_NAME = "IR_DEMO"
AWS_IP = "localhost"


def delete_core(core=CORE_NAME):
    print(os.system('/opt/homebrew/Cellar/solr/8.9.0/bin/solr delete -c {core}'.format(core=core)))


def create_core(core=CORE_NAME):
    print(os.system(
        '/opt/homebrew/Cellar/solr/8.9.0/bin/solr create -c {core} -n data_driven_schema_configs'.format(
            core=core)))


# collection

collection = [{
    "id": 1,
    "first_name": "Chickie",
    "last_name": "Proven",
    "email": "cproven0@alexa.com",
    "age": 77,
    "pincodes": [2121212, 3232323]
}, {
    "id": 2,
    "first_name": "Dex",
    "last_name": "Bofield",
    "email": "dbofield1@about.com",
    "age": 88,
    "pincodes": [2121212, 3232323]
}, {
    "id": 3,
    "first_name": "Saba",
    "last_name": "Ace",
    "email": "sace2@craigslist.org",
    "age": 55,
    "pincodes": [2121212, 3232323]
}, {
    "id": 4,
    "first_name": "Hymie",
    "last_name": "Patterfield",
    "email": "hpatterfield3@plala.or.jp",
    "age": 22,
    "pincodes": [2121212, 3232323]
}, {
    "id": 5,
    "first_name": "Chiarra",
    "last_name": "Cornils",
    "email": "ccornils4@patch.com",
    "age": 23,
    "pincodes": [2121212, 3232323]
}]


class Indexer:
    def __init__(self):
        self.solr_url = f'http://{AWS_IP}:8983/solr/'
        self.connection = pysolr.Solr(self.solr_url + CORE_NAME, always_commit=True, timeout=5000000)

    def do_initial_setup(self):
        delete_core()
        create_core()

    def create_documents(self, docs):
        print(self.connection.add(docs))

    def add_fields(self):
        data1 = {
            "add-field": [
                {
                    "name": "first_name",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "last_name",
                    "type": "string",
                    "multiValued": False
                }, {
                    "name": "email",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "age",
                    "type": "pint",
                    "multiValued": False
                },
                {
                    "name": "pincodes",
                    "type": "plongs",
                    "multiValued": True
                },
            ]
        }
        data = {
            "add-field": [
                {
                    "name": "poi_name",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "poi_id",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "verified",
                    "type": "boolean",
                    "multiValued": False
                },
                {
                    "name": "country",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "id",
                    "type": "plong",
                    "multiValued": False
                },
                {
                    "name": "replied_to_tweet_id",
                    "type": "plong",
                    "multiValued": False
                },
                {
                    "name": "replied_to_user_id",
                    "type": "plong",
                    "multiValued": False
                },
                {
                    "name": "reply_text",
                    "type": "text_general",
                    "multiValued": True
                },
                {
                    "name": "reply_text",
                    "type": "text_general",
                    "multiValued": True
                },
                {
                    "name": "tweet_text",
                    "type": "text_general",
                    "multiValued": True
                },
                {
                    "name": "tweet_lang",
                    "type": "plong",
                    "multiValued": False
                },
                {
                    "name": "hashtags",
                    "type": "string",
                    "multiValued": True
                },
                {
                    "name": "mentions",
                    "type": "string",
                    "multiValued": True
                },
                {
                    "name": "tweet_urls",
                    "type": "strings",
                    "multiValued": True
                },
                {
                    "name": "tweet_emoticons",
                    "type": "string",
                    "multiValued": True
                },
                {
                    "name": "tweet_date",
                    "type": "pdate",
                    "multiValued": False
                },
            ]
        }

        print(requests.post(self.solr_url + CORE_NAME + "/schema", json=data).json())


if __name__ == "__main__":
    i = Indexer()
    i.do_initial_setup()
    i.add_fields()
    #i.create_documents(collection)
