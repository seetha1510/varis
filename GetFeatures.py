# -*- coding: utf-8 -*-
"""
Created on Thu Mar  3 17:26:01 2022

@author: Owner
"""

# import libraries
import os
from operator import index
import numpy as np
import pandas as pd
import sqlite3
#from rake_nltk import Rake
import nltk
import yake
import time
#nltk.download('stopwords')
#nltk.download('punkt')

# connect to sql server
# conn = sqlite3.connect(":memory:")
databaseName = "UAT_2.db"
conn = sqlite3.connect(databaseName)
cursor = conn.cursor()

def yake_features(text, max_words=10, duplicates=0.5, phrase_size=3):
    #ystart = time.time()
    kw_extractor = yake.KeywordExtractor()
    language = "en"
    max_ngram_size = phrase_size
    deduplication_threshold = duplicates
    numOfKeywords = max_words
    custom_kw_extractor = yake.KeywordExtractor(lan=language, n=max_ngram_size, dedupLim=deduplication_threshold, top=numOfKeywords, features=None)
    keywords = custom_kw_extractor.extract_keywords(text)
    b = []
    for kw in keywords:
        #b = b + kw[0] + ", "
        b.append(kw[0])
        #b.append(" , ")
    c = ', '.join(b)
    #print(a)
    #yend = time.time()
    #print("--- Yake time: " + str(yend - ystart) + " seconds ---")
    #print(b)
    #print(c)
    return c

try:
    # create features table
    features_table = '''
    CREATE TABLE features(
        SKU int,
        Features text,
        FOREIGN KEY(SKU) REFERENCES products(SKU)
    )
    '''
    cursor.execute(features_table)
    #print("Ping1")
except:
    
    try: #delete existing table and remake it
        cursor.execute("DROP TABLE IF EXISTS features")
        
        features_table = '''
        CREATE TABLE features(
            SKU int,
            Features text,
            FOREIGN KEY(SKU) REFERENCES products(SKU)
        )
        '''
        cursor.execute(features_table)
        #print("Ping2")
    except:
        conn.close()
        #print("Ping3")


try:
    #Get all of the Combined Descriptions
    #rows = pd.read_sql_query("SELECT COUNT() FROM products ", conn)
    #descriptions = pd.read_sql_query("SELECT CombinedDescription FROM products ", conn)
    cursor.execute("SELECT SKU, CombinedDescription FROM products")  # execute a simple SQL select query
    descriptions = cursor.fetchall()
    
    #print("Ping4")
    #Initilize a Rake variable
    #r = Rake()
    
    i = 0
    start2_time = time.time()
    for d in descriptions:
        print(i)
        i = i + 1
        """
        r.extract_keywords_from_text(d[1])
        k = r.get_ranked_phrases()
        k = ', '.join(k)
        """
        #print(d[0])
        k = yake_features(d[1])
        #print(k)
        
        a = int(d[0])
        cursor.execute('''
                       INSERT INTO features (SKU, Features)
                       VALUES (?,?)
                       ''',
                       tuple((a, k))
                       )
        #print("Hello there")
    conn.commit()
    end2_time = time.time()
    print("--- Feature Extraction Time: " + str(end2_time-start2_time) + " seconds ---")    
except:
    conn.close()
    #print("Ping5")

#Close the connection
conn.close()
#print("Ping6")