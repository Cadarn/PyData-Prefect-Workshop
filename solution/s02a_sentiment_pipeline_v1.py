from pathlib import Path
import re

import numpy as np
import pandas as pd

import spacy
from spacytextblob.spacytextblob import SpacyTextBlob
from nltk.stem import WordNetLemmatizer

nlp = spacy.load('en_core_web_sm')
nlp.add_pipe("spacytextblob")

wordLemmatizer = WordNetLemmatizer()

DATA_ROOT = Path("./data")
AIRLINE_FILE = "airline_tweets.csv"


def load_airline_tweets(data_path: Path)->pd.DataFrame:
    """Load data from the airline tweets dataset"""
    df = pd.read_csv(data_path)
    return df


def calc_sentiment(text: str)->float:
    """Calculate text sentiment based on the spacytextblob polarity model"""
    doc = nlp(text)
    return doc._.blob.polarity

# Text preprocessing
# Defining dictionary containing all emojis with their meanings.
emojis = {':)': 'smile', ':-)': 'smile', ';d': 'wink', ':-E': 'vampire', ':(': 'sad', 
          ':-(': 'sad', ':-<': 'sad', ':P': 'raspberry', ':O': 'surprised',
          ':-@': 'shocked', ':@': 'shocked',':-$': 'confused', ':\\': 'annoyed', 
          ':#': 'mute', ':X': 'mute', ':^)': 'smile', ':-&': 'confused', '$_$': 'greedy',
          '@@': 'eyeroll', ':-!': 'confused', ':-D': 'smile', ':-0': 'yell', 'O.o': 'confused',
          '<(-_-)>': 'robot', 'd[-_-]b': 'dj', ":'-)": 'sadsmile', ';)': 'wink', 
          ';-)': 'wink', 'O:-)': 'angel','O*-)': 'angel','(:-D': 'gossip', '=^.^=': 'cat'}

## Defining set containing all stopwords in english.
stopwordlist = ['a', 'about', 'above', 'after', 'again', 'ain', 'all', 'am', 'an',
             'and','any','are', 'as', 'at', 'be', 'because', 'been', 'before',
             'being', 'below', 'between','both', 'by', 'can', 'd', 'did', 'do',
             'does', 'doing', 'down', 'during', 'each','few', 'for', 'from', 
             'further', 'had', 'has', 'have', 'having', 'he', 'her', 'here',
             'hers', 'herself', 'him', 'himself', 'his', 'how', 'i', 'if', 'in',
             'into','is', 'it', 'its', 'itself', 'just', 'll', 'm', 'ma',
             'me', 'more', 'most','my', 'myself', 'now', 'o', 'of', 'on', 'once',
             'only', 'or', 'other', 'our', 'ours','ourselves', 'out', 'own', 're',
             's', 'same', 'she', "shes", 'should', "shouldve",'so', 'some', 'such',
             't', 'than', 'that', "thatll", 'the', 'their', 'theirs', 'them',
             'themselves', 'then', 'there', 'these', 'they', 'this', 'those', 
             'through', 'to', 'too','under', 'until', 'up', 've', 'very', 'was',
             'we', 'were', 'what', 'when', 'where','which','while', 'who', 'whom',
             'why', 'will', 'with', 'won', 'y', 'you', "youd","youll", "youre",
             "youve", 'your', 'yours', 'yourself', 'yourselves']

def lowercase_text(text: str) -> str:
    """
    Convert all characters in the text to lowercase and strip leading/trailing whitespace.

    Args:
        text (str): The input text to be converted.

    Returns:
        str: The text converted to lowercase with leading/trailing whitespace removed.
    """
    return text.lower().strip()


def strip_url(text: str) -> str:
    """
    Replace web addresses in the text with the placeholder 'WEBADDRESS'.

    Args:
        text (str): The input text containing URLs.

    Returns:
        str: The text with URLs replaced by 'WEBADDRESS'.
    """
    pattern = r'(https?://[^\s]+|www\.[^\s]+)'
    clean_text = re.sub(pattern, 'WEBADDRESS', text)
    return clean_text


def strip_user(text: str) -> str:
    """
    Replace user handles (mentions) in the text with the placeholder 'USERHANDLE'.

    Args:
        text (str): The input text containing user handles.

    Returns:
        str: The text with user handles replaced by 'USERHANDLE'.
    """
    pattern = r'@[^\s]+'
    clean_text = re.sub(pattern, 'USERHANDLE', text)
    return clean_text


def replace_emoji(text: str) -> str:
    """
    Replace emojis in the text with their English word equivalents followed by 'EMOJI'.

    Args:
        text (str): The input text containing emojis.

    Returns:
        str: The text with emojis replaced by their English word equivalents.
    """
    for emoji, description in emojis.items():
        text = text.replace(emoji, f"{description} EMOJI")
    return text


def lemmatize_text(text: str) -> str:
    """
    Lemmatize words in the text.

    Args:
        text (str): The input text containing words to lemmatize.

    Returns:
        str: The text with words lemmatized.
    """
    words = [wordLemmatizer.lemmatize(word) for word in text.split()]
    return ' '.join(words)


def process_text(text: str) -> str:
    """
    Pre-process text for sentiment analysis by converting to lowercase, stripping URLs and user handles, 
    replacing emojis, and lemmatizing the text.

    Args:
        text (str): The input text to be processed.

    Returns:
        str: The processed text.
    """
    text = lowercase_text(text)
    text = strip_url(text)
    text = strip_user(text)
    text = replace_emoji(text)
    text = lemmatize_text(text)
    return text



if __name__ == "__main__":
    # Perform some rudimentary twitter sentiment analysis
    tweets = load_airline_tweets(DATA_ROOT / AIRLINE_FILE)
    tweets.loc[:, "calc_sentiment"] = tweets.text.map(calc_sentiment)
    print(len(tweets))
    print(tweets.columns)
    print(tweets.sample(1))
    for idx, tweet in tweets.sample(10).iterrows():
        clean_text = process_text(tweet["text"])
        doc = nlp(clean_text)
        print(tweet["text"])
        print(clean_text)
        print(idx, tweet["airline_sentiment"], tweet["text"], doc._.blob.polarity)
        print(40*"=")
    print(tweets.calc_sentiment.describe())
    print(tweets.groupby("airline_sentiment")["calc_sentiment"].describe())