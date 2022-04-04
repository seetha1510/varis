# -*- coding: utf-8 -*-
"""
Created on Thu Mar  3 15:12:37 2022

@author: Carlos Rodriguez
"""

#import spacy

import time

import yake

from bs4 import BeautifulSoup

from rake_nltk import Rake

import nltk
#nltk.download('stopwords')
#nltk.download('punkt')


sentence = 'At-A-Glance Tropical Escape Monthly Wall Calendar - Julian Dates - Monthly - 1 Year - January 2021 till December 2021 - 1 Month Single Page Layout - 15" x 12" Sheet Size - 2" x 1" Block - Wire Bound - Wall Mountable - Blue'
sent2 = 'Adams® Bill of Sale Protect both the buyer and seller with these 4 easy-to-use legal forms <ul><li>Buy or sell personal property confidently, and customize the forms to suit your situation. Use to sell or transfer an asset, specify the amount of consideration paid for transfer of title and date of purchase, or disclose information about the asset being transferred and guarantee the item is free from claims and offsets.</li> <li>Includes step-by-step instructions.</li></ul>'
sent3 = 'this sentence is short'
sent4 = 'This printer is the nicest one on the market. We are the absolute best in world and the competion sucks. The printer is blue, big, very loud, and kind of sucks, but still better than anything else'
sent5 = sentence + sent2 + sent3 + sent4

#tokens = 


def rake_features(text):
    rstart = time.time()
    # Uses stopwords for english from NLTK, and all puntuation characters by
    # default
    r = Rake()
    
    # Extraction given the text.
    r.extract_keywords_from_text(text)
    
    # Extraction given the list of strings where each string is a sentence.
    #r.extract_keywords_from_sentences(<list of sentences>)
    
    # To get keyword phrases ranked highest to lowest.
    k = r.get_ranked_phrases()
    #print(k)
    
    # To get keyword phrases ranked highest to lowest with scores.
    c = r.get_ranked_phrases_with_scores()
    print(k)
    rend = time.time()
    print("--- Rake time: " + str(rend - rstart) + " seconds ---")
"""


#"""
def yake_features(text, max_words=3, duplicates=0.5, phrase_size=3):
    ystart = time.time()
    kw_extractor = yake.KeywordExtractor()
    language = "en"
    max_ngram_size = phrase_size
    deduplication_threshold = duplicates
    numOfKeywords = max_words
    custom_kw_extractor = yake.KeywordExtractor(lan=language, n=max_ngram_size, dedupLim=deduplication_threshold, top=numOfKeywords, features=None)
    keywords = custom_kw_extractor.extract_keywords(text)
    a = []
    for kw in keywords:
        a.append(kw[0])
    print(a)
    yend = time.time()
    print("--- Yake time: " + str(yend - ystart) + " seconds ---")
#"""

def otherHTML(data):
    #data = BeautifulSoup(data, 'lxml').text
    #for x in data.index:
        #txt = str(row[1])
        #data.loc[x,'description']= 
        #print(txt)
    #print(df['description'].head())
    
    return BeautifulSoup(data, 'lxml').text 

def cleanedYake(text):
    yake_features(otherHTML(text))

rake_features(sent5)

yake_features(sent5)

cleanedYake(sent5)


"""
nlp = spacy.load("en_core_web_sm")
doc = nlp(sentence)
print(doc.ents)
"""
