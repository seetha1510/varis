import os
import sys
from operator import index
import numpy as np
import pandas as pd
import sqlite3
from rake_nltk import Rake
import nltk
import traceback
import time
import yake

def yake_features(text, max_words=60, duplicates=0.5, phrase_size=3):

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
    a = ', '.join(a)
    
    return a

def main(dbName=sys.argv[1]):

    # connect to sql server

    databaseName = dbName
    conn = sqlite3.connect(databaseName)
    cursor = conn.cursor()
    
    try: # create features table  
        category_features_table = '''
        CREATE TABLE category_features(
            CAT_ID text PRIMARY KEY,
            CAT_Superset_Description text,
            CAT_Features text,
            FOREIGN KEY(CAT_ID) REFERENCES categories(CAT_ID)
        )
        '''
        cursor.execute(category_features_table)
        print("category_features table created")
    except:
        
        try: #delete existing table and remake it
            cursor.execute("DROP TABLE IF EXISTS category_features") 
            category_features_table = '''
            CREATE TABLE category_features(
                CAT_ID text PRIMARY KEY,
                CAT_Superset_Description text,
                CAT_Features text,
                FOREIGN KEY(CAT_ID) REFERENCES categories(CAT_ID)
            )
            '''
            cursor.execute(category_features_table)
            print("category_features table created")
        except:
            conn.close()
            print("Unable to create category_features table, closing connection.")
    
    
    try:# aggregate features from the classes and redo a keyword search on them to create category features
        cursor.execute("CREATE TEMP VIEW category_class_features " +
                       "AS " + 
                       "SELECT " +
                           "classes.CAT_ID, " +
                           "class_features.CLS_Features " +
                       "FROM " +
                           "class_features " +
                            "INNER JOIN classes USING(CLS_ID); ")
        
        cursor.execute("SELECT CAT_ID FROM categories")  # execute a simple SQL select query
        categories = cursor.fetchall()
        

        i = 1
 
        start2_time = time.time()
        for cat in categories:
          
            sys.stdout.write("\rCategory proccessed: %d" % i)
            i = i+1
            
            if(cat[0]== None):
                continue 
            
            cursor.execute("SELECT group_concat(CLS_Features) " + 
                           "FROM category_class_features " +
                           "WHERE CAT_ID = '" +
                           str(cat[0]) +
                           "' GROUP BY CAT_ID")
            cat_superset_desc = cursor.fetchall()
           
            k = yake_features(cat_superset_desc[0][0])
            cursor.execute('''
                           INSERT INTO category_features(CAT_ID, CAT_Superset_Description,CAT_Features)
                           VALUES (?,?,?)
                           ''',
                           tuple((str(cat[0]), str(cat_superset_desc[0][0]), k))
                           )
        conn.commit()
        end2_time = time.time()
        print("--- Feature Extraction Time: " + str(end2_time-start2_time) + " seconds ---")
    except:
        conn.close()
        print("There was an issue with the cats")
        traceback.print_exc()
        
    #Close the connection
    conn.close()
    
if __name__ == "__main__":
    main()