import os
import sys
from operator import index
import numpy as np
import pandas as pd
import sqlite3
import nltk
import yake
import time

from GetConnection import getConnection

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
def createFeaturesTable(conn):
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
    except sqlite3.OperationalError as e:
        # if str(e) == ""
        print("Features table already exists, deleting and re-creating")
        print(e)
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
        except Exception as e:
            print("entered except in createFeaturesTable")
            raise e
            conn.close()
    conn.commit()

def main(dbName):
    
    # connect to sql server
    conn = getConnection(dbName)
    cursor = conn.cursor()
    
    createFeaturesTable(conn)
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
        print("entered except in main")
        conn.close()
  
    
    #Close the connection
    print("closing except")
    conn.close()

    
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("database_name")
    args = parser.parse_args()
    main(args.database_name)