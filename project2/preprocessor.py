'''
@author: Sougata Saha
Institute: University at Buffalo
'''

import collections
from nltk.stem import PorterStemmer
import re
from nltk.corpus import stopwords
import nltk
nltk.download('stopwords')


class Preprocessor:
    def __init__(self):
        self.stop_words = set(stopwords.words('english'))
        self.ps = PorterStemmer()

    def get_doc_id(self, doc):
        """ Splits each line of the document, into doc_id & text.
            Already implemented"""
        arr = doc.split("\t")
        return int(arr[0]), arr[1]

    def tokenizer(self, text):
        """ Implement logic to pre-process & tokenize document text.
            Write the code in such a way that it can be re-used for processing the user's query.
            To be implemented."""
        text = text.strip()
        text = re.sub(r"[^a-zA-Z0-9]+", ' ', text)
        text = text.lower()
        text = ' '.join(text.split())
        _text_list = text.split(' ')
        _text_list = [_text for _text in _text_list if _text]
        _list_stop_words = [word for word in _text_list if word not in self.stop_words]
        _token_list = [self.ps.stem(word) for word in _list_stop_words]
        return _token_list
