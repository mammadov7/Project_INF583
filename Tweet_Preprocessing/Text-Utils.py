"""
In this script we will store all the preprocesing utils.
"""
import pandas as pd
import numpy as np
import re # regular expressions to clean up text
import nltk
from nltk.corpus import stopwords # to filter words
nltk.download('stopwords', quiet=True)
nltk.download('wordnet', quiet=True) # to lemmatize words

# time functions for time stamp
from tqdm import tqdm
from datetime import datetime 

## Functions for preprocessing words in the tweets

def CleanUp(txt):
    """
    Clean up text by removing line skips, URLs & non alphanumeric characters (punctuation signs, emojis).
    """
    newLine = re.compile(r'\n')
    oneLineTxt = newLine.sub(r' ', txt)
    pattern = re.compile(r'(https?://\S+|www\.\S+)|([^0-9A-Za-z ])')
    cleanTxt = pattern.sub(r' ', oneLineTxt)  # substitute pattern occurrences by single space
    return cleanTxt

def Tokenize(txt):
    """
    Split text into list of tokens (lowercase words or numbers).
    """
    return txt.lower().split()

def FilterWords(lst):
    """
    Remove stop words from a list of words.
    """
    stopWords = set(stopwords.words('english'))
    return [word for word in lst if not word in stopWords]

def Lemmatize(lst):
    """
    Reduce words to their lemmas.
    """
    wn = nltk.WordNetLemmatizer()
    return [wn.lemmatize(word) for word in lst]## Functions for preprocessing words in the tweats

def SortUnique(lst):
    """
    Sort list of words and remove duplicates.
    """
    return list(np.unique(lst))

def ProcessText(txt):
    """
    Apply complete preprocessing on text
    """
    return SortUnique(Lemmatize(FilterWords(Tokenize(CleanUp(txt)))))

def FullCleaning(txt):
    """
    Return a cleaned version of the txt
    """
    lst = Lemmatize(FilterWords(Tokenize(CleanUp(txt))))
    return ' '.join(lst)

def ComputeCleanTexts(series):
    """
    Turns a text series into a clean text series.
    """
    cleanTexts = series.copy()

    for index, text in tqdm(series.items()):
        cleanTexts[index] = FullCleaning(text)

    return cleanTexts

## Functions for preprocessing timestamp
def ChangeTimeFormat(df):
    df_copy = df.copy()
    
    if "timestamp" in df_copy.columns:
        
        month = []
        day = []
        week_day = []
        hour = []
        
        # loop over all the tweets in df
        for i in tqdm(range(len(df))):
            
            tweet = df_copy.iloc[i]
            
            t = datetime.fromtimestamp(tweet["timestamp"]/1000)
            
            month.append(t.month)
            day.append(t.day)
            week_day.append(t.weekday())
            hour.append(t.hour)
            
        df_copy["month"] = month
        df_copy["day"] = day
        df_copy["week_day"] = week_day
        df_copy["hour"] = hour
        
        df_copy = df_copy.drop(columns=["timestamp"])
        
        return df_copy

def CountCommaSeparated(df, field):
    count = []
    for string in tqdm(df[field]):
        if(pd.isna(string)):
            count.append(0)
        else:
            count.append(len(string.split(',')))
    return count

def CountSpaceSeperated(df, field):
    count = []
    for string in tqdm(df[field]):
        if(pd.isna(string)):
            count.append(0)
        else:
            count.append(len(string.split(' ')))
    return count

