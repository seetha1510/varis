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

def yake_features(text, max_words=60, duplicates=0.5, phrase_size=3):
    # text = text set the features are being extracted from
    # max_words = max number of words allowed to be extracted from this text, set to 60 at the category level
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

def createCategoryFeaturesTable(conn):
    cursor = conn.cursor()
    try: # create features table with CAT_ID, superset description of the category, extracted features of the category and is linked to the category table   
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
        
        try: # delete existing table and recreate it
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
        except: # any other exception
            conn.close()
            print("Unable to create category_features table, closing connection.")
        conn.commit()

def generateCategoryFeatures(conn):
    cursor = conn.cursor()
    try: # create a temp table that contains all the classes' categories with the classes' extracted features
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
 
        # for each category in the database extract features using yake module
        start2_time = time.time()
        for cat in categories:
          
            sys.stdout.write("\rCategory proccessed: %d" % i)
            i = i+1
            
            if(cat[0]== None):
                continue 
            
            # run sql command to get all extracted features of the classes under each category grouped together -> the superset description for each category
            cursor.execute("SELECT group_concat(CLS_Features) " + 
                           "FROM category_class_features " +
                           "WHERE CAT_ID = '" +
                           str(cat[0]) +
                           "' GROUP BY CAT_ID")
            cat_superset_desc = cursor.fetchall()

            # runs yake on the superset of features of all classes under the given category and returns a set of keywords
            k = yake_features(cat_superset_desc[0][0])
            
            # insert into the category features table using CAT_ID of category and the extracted features
            cursor.execute('''
                           INSERT INTO category_features(CAT_ID, CAT_Superset_Description,CAT_Features)
                           VALUES (?,?,?)
                           ''',
                           tuple((str(cat[0]), str(cat_superset_desc[0][0]), k))
                           )

        # commit to database once all the categories are processed
        conn.commit()
        end2_time = time.time()
        print("--- Feature Extraction Time: " + str(end2_time-start2_time) + " seconds ---")
    except:
        conn.close()
        print("There was an issue with the cats")
        traceback.print_exc()

def main(dbName):

    # connect to sql server
    conn = getConnection(dbName)
    cursor = conn.cursor()

    # create CategoryFeatures table to store category features   
    createCategoryFeaturesTable(conn)
    
    # extract features for the categories
    generateCategoryFeatures(conn)
      
    #Close the connection
    conn.close()
    
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("database_name")
    args = parser.parse_args()

    main(args.database_name)