import os
import sys
from operator import index
import numpy as np
import pandas as pd
import sqlite3
from rake_nltk import Rake
import nltk
import traceback
import yake
import time

from GetConnection import getConnection

def yake_features(text, max_words=20, duplicates=0.5, phrase_size=3):
    # text = text set the features are being extracted from
    # max_words = max number of words allowed to be extracted from this text, set to 20 at the subclass level
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
    a = []
    for kw in keywords:
        a.append(kw[0])
    a = ', '.join(a)
    # comma separated keywords are returned
    return a

def createSubclassFeaturesTable(conn):
    cursor = conn.cursor()
    try: # create features table with SUB_ID, superset description of the subclass, extracted features of the subclass and is linked to the subclasses table
        
        subclass_features_table = '''
        CREATE TABLE subclass_features(
            SUB_ID text PRIMARY KEY,
            SUB_Superset_Description text,
            SUB_Features text,
            FOREIGN KEY(SUB_ID) REFERENCES subclasses(SUB_ID)
        )
        '''
        cursor.execute(subclass_features_table)
        print("subclass_features table created")
    except:
        
        try: # delete existing table and recreate it
            cursor.execute("DROP TABLE IF EXISTS subclass_features") 
            subclass_features_table = '''
            CREATE TABLE subclass_features(
                SUB_ID text PRIMARY KEY,
                SUB_Superset_Description text,
                SUB_Features text,
                FOREIGN KEY(SUB_ID) REFERENCES subclasses(SUB_ID)
            )
            '''
            cursor.execute(subclass_features_table)
            print("subclass_features table created")
        except: # any other exception
            conn.close()
            print("Unable to create subclass_features table, closing connection.")
        conn.commit()

def generateSubclassFeatures(conn):
    cursor = conn.cursor()
    try: 
        # create a temp table that contains all the products' subclass along with the products' extracted features
        cursor.execute("CREATE TEMP VIEW subclass_product_features " +
                       "AS " + 
                       "SELECT " +
                           "products.SubClass, " +
                           "features.Features " +
                       "FROM " +
                           "products " +
                            "INNER JOIN features USING(SKU); ")
        
        cursor.execute("SELECT SUB_ID FROM subclasses")  # execute a simple SQL select query
        subclasses = cursor.fetchall()
      
        i = 1
  
        start2_time = time.time()

        # for each subclass in the database extract features using yake module
        for sub in subclasses:
            # print out current number of subclasses that are processed to let the user know the progress
            sys.stdout.write("\rSubclass proccessed: %d" % i)
            
            i = i+1
        
            if(sub[0]== None):
                continue
            
            # run sql command to get all extracted features of the items under each subclass grouped together -> the superset description for each subclass
            cursor.execute("SELECT group_concat(Features) " + 
                           "FROM subclass_product_features " +
                           "WHERE SubClass = '" +
                           str(sub[0]) +
                           "' GROUP BY SubClass")
            sub_superset_desc = cursor.fetchall()

            # runs yake on the superset of features of all items under the given subclass and returns a set of keywords
            k = yake_features(sub_superset_desc[0][0])
            
            # insert into the subclass features table using SUB_ID of subclass and the extracted features
            cursor.execute('''
                           INSERT INTO subclass_features(SUB_ID, SUB_Superset_Description, SUB_Features)
                           VALUES (?,?,?)
                           ''',
                           tuple((str(sub[0]), str(sub_superset_desc[0][0]), k))
                           )
        # commit to database once all the subclasses are processed
        conn.commit()
        end2_time = time.time()
        print("--- Feature Extraction Time: " + str(end2_time-start2_time) + " seconds ---")
    except:
        conn.close()
        print("There was an issue with the subclasses")
        traceback.print_exc()

def main(dbName):
    
    # connect to sql server
    conn = getConnection(dbName)
    cursor = conn.cursor()
    
    # create SubclassFeatures table to store subclass features
    createSubclassFeaturesTable(conn)

    # extract features for the subclasses
    generateSubclassFeatures(conn)

    
    #Close the connection
    conn.close()
    
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("database_name")
    args = parser.parse_args()

    main(args.database_name)