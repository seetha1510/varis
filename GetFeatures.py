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
    # text = text set the features are being extracted from
    # max_words = max number of words allowed to be extracted from this text, set to 10 at the product level
    # duplicates = the duplication factor allowed set to 0.5 to allow some repetition of important words
    # phrase size = the maximum size of phrase allowed to be extracted, set to 3
    kw_extractor = yake.KeywordExtractor()
    language = "en"
    max_ngram_size = phrase_size
    deduplication_threshold = duplicates
    numOfKeywords = max_words
    
    # setting the custom constraints on the keyword extractor module
    custom_kw_extractor = yake.KeywordExtractor(lan=language, n=max_ngram_size, dedupLim=deduplication_threshold, top=numOfKeywords, features=None)
    keywords = custom_kw_extractor.extract_keywords(text)
    
    # extracted keywords are formatted in appropriate form to be database friendly while insertion
    b = []
    for kw in keywords:

        b.append(kw[0])

    c = ', '.join(b)

    # comma separated keywords are returned
    return c

def createFeaturesTable(conn):
    cursor = conn.cursor()
    try:
        # create features table that contain SKU of product, extracted Features, and is linked to the original products table
        features_table = '''
        CREATE TABLE features(
            SKU int,
            Features text,
            FOREIGN KEY(SKU) REFERENCES products(SKU)
        )
        '''
        cursor.execute(features_table)
    except sqlite3.OperationalError as e:
        print("Features table already exists, deleting and re-creating")
        print(e)
        try: # delete existing table and recreate it
            cursor.execute("DROP TABLE IF EXISTS features")
            
            features_table = '''
            CREATE TABLE features(
                SKU int,
                Features text,
                FOREIGN KEY(SKU) REFERENCES products(SKU)
            )
            '''
            cursor.execute(features_table)
        except Exception as e: # any other exception
            print("entered except in createFeaturesTable")
            raise e
    conn.commit()

def main(dbName):
    
    # connect to sql server
    conn = getConnection(dbName)
    cursor = conn.cursor()
    
    # create Features table to store product features
    createFeaturesTable(conn)
    try:
        #Get all of the Combined Descriptions
        cursor.execute("SELECT SKU, CombinedDescription FROM products")  # execute a simple SQL select query
        descriptions = cursor.fetchall()
      
        
        i = 0
        start2_time = time.time()
        # for each item in the database extract features using yake module
        for d in descriptions:
            # print out current item that is being processed to let the user know the progress
            sys.stdout.write("\r%d"% i)
            i = i + 1
           
           # runs yake on the the combined description of item and returns a set of keywords
            k = yake_features(d[1])

            a = int(d[0])
            # insert into the features table using SKU of item and the extracted features
            cursor.execute('''
                           INSERT INTO features (SKU, Features)
                           VALUES (?,?)
                           ''',
                           tuple((a, k))
                           )
       
       # commit to database once all the items are processed
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