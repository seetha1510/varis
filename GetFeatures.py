import os
import sys
from operator import index
import numpy as np
import pandas as pd
import sqlite3
import nltk
import yake
import time



def yake_features(text, max_words=10, duplicates=0.5, phrase_size=3):

    kw_extractor = yake.KeywordExtractor()
    language = "en"
    max_ngram_size = phrase_size
    deduplication_threshold = duplicates
    numOfKeywords = max_words
    custom_kw_extractor = yake.KeywordExtractor(lan=language, n=max_ngram_size, dedupLim=deduplication_threshold, top=numOfKeywords, features=None)
    keywords = custom_kw_extractor.extract_keywords(text)
    b = []
    for kw in keywords:

        b.append(kw[0])

    c = ', '.join(b)

    return c


def main(dbName=sys.argv[1]):
    
    # connect to sql server
    databaseName = dbName
    conn = sqlite3.connect(databaseName)
    cursor = conn.cursor()
    
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
        except:
            conn.close()
    
    
    try:
        #Get all of the Combined Descriptions
        cursor.execute("SELECT SKU, CombinedDescription FROM products")  # execute a simple SQL select query
        descriptions = cursor.fetchall()
      
        
        i = 0
        start2_time = time.time()
        for d in descriptions:
            sys.stdout.write("\r%d"% i)
            i = i + 1
           
            k = yake_features(d[1])

            a = int(d[0])
            cursor.execute('''
                           INSERT INTO features (SKU, Features)
                           VALUES (?,?)
                           ''',
                           tuple((a, k))
                           )
       
        conn.commit()
        end2_time = time.time()
        print("--- Feature Extraction Time: " + str(end2_time-start2_time) + " seconds ---")    
    except:
        conn.close()
  
    
    #Close the connection
    conn.close()

    
if __name__ == "__main__":
    main()