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

from GetConnection import getConnection

def yake_features(text, max_words=40, duplicates=0.5, phrase_size=3):

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
def createClassFeaturesTable(conn):
    cursor = conn.cursor()
    try: # create features table  
        class_features_table = '''
        CREATE TABLE class_features(
            CLS_ID text PRIMARY KEY,
            CLS_Superset_Description text,
            CLS_Features text,
            FOREIGN KEY(CLS_ID) REFERENCES classes(CLS_ID)
        )
        '''
        cursor.execute(class_features_table)
        print("class_features table created")
    except:
        
        try: #delete existing table and remake it
            cursor.execute("DROP TABLE IF EXISTS class_features") 
            class_features_table = '''
            CREATE TABLE class_features(
                CLS_ID text PRIMARY KEY,
                CLS_Superset_Description text,
                CLS_Features text,
                FOREIGN KEY(CLS_ID) REFERENCES classes(CLS_ID)
            )
            '''
            cursor.execute(class_features_table)
            print("class_features table created")
        except:
            conn.close()
            print("Unable to create class_features table, closing connection.")
        conn.commit()

def generateClassFeatures(conn):
    cursor = conn.cursor()
    try:# aggregate features from the subclasses and redo a keyword search on them to create class features
        cursor.execute("CREATE TEMP VIEW class_subclass_features " +
                       "AS " + 
                       "SELECT " +
                           "subclasses.CLS_ID, " +
                           "subclass_features.SUB_Features " +
                       "FROM " +
                           "subclass_features " +
                            "INNER JOIN subclasses USING(SUB_ID); ")
        
        cursor.execute("SELECT CLS_ID FROM classes")  # execute a simple SQL select query
        classes = cursor.fetchall()
        

        i = 1
       
        start2_time = time.time()
        for cla in classes:
            
            sys.stdout.write("\rClass proccessed: %d" % i)
            i = i+1
            
            if(cla[0]== None):
                continue 
            
            cursor.execute("SELECT group_concat(SUB_Features) " + 
                           "FROM class_subclass_features " +
                           "WHERE CLS_ID = '" +
                           str(cla[0]) +
                           "' GROUP BY CLS_ID")
            cls_superset_desc = cursor.fetchall()

            k = yake_features(cls_superset_desc[0][0])
            cursor.execute('''
                           INSERT INTO class_features(CLS_ID, CLS_Superset_Description,CLS_Features)
                           VALUES (?,?,?)
                           ''',
                           tuple((str(cla[0]), str(cls_superset_desc[0][0]), k))
                           )
        conn.commit()
        end2_time = time.time()
        print("--- Feature Extraction Time: " + str(end2_time-start2_time) + " seconds ---")
    except:
        conn.close()
        print("There was an issue with the classes")
        traceback.print_exc()

def main(dbName):
    
    # connect to sql server
    conn = getConnection(dbName)
    cursor = conn.cursor()

    createClassFeaturesTable(conn)
    generateClassFeatures(conn)
        
    #Close the connection
    conn.close()
    
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("database_name")
    args = parser.parse_args()

    main(args.database_name)