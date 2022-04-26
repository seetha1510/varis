import os
import sys
from operator import index
import numpy as np
import pandas as pd
import sqlite3
from pyrfc3339 import generate
from rake_nltk import Rake
import nltk
import traceback
import time
import yake

from GetConnection import getConnection

def yake_features(text, max_words=80, duplicates=0.5, phrase_size=3):
    # text = text set the features are being extracted from
    # max_words = max number of words allowed to be extracted from this text, set to 80 at the segment level
    # duplicates = the duplication factor allowed set to 0.5 to allow some repetition of important words
    # phrase size = the maximum size of phrase allowed to be extracted, set to 3
    kw_extractor = yake.KeywordExtractor()
    language = "en"
    max_ngram_size = phrase_size
    deduplication_threshold = duplicates
    numOfKeywords = max_words
    
    # setting the custom constraints on the keyword extractor module
    custom_kw_extractor = yake.KeywordExtractor(lan=language, n=max_ngram_size, dedupLim=deduplication_threshold, top=numOfKeywords, features=None)
    
    # extracted keywords are formatted in appropriate form to be database friendly while insertion
    keywords = custom_kw_extractor.extract_keywords(text)
    a = []
    for kw in keywords:
        a.append(kw[0])
    a = ', '.join(a)
    
    # comma separated keywords are returned
    return a

def createSegmentFeaturesTable(conn):
    cursor = conn.cursor()
    try: # create features table with SEG_ID, superset description of the segment, extracted features of the segment and is linked to the segment table   
        segment_features_table = '''
        CREATE TABLE segment_features(
            SEG_ID text PRIMARY KEY,
            SEG_Superset_Description text,
            SEG_Features text,
            FOREIGN KEY(SEG_ID) REFERENCES segments(SEG_ID)
        )
        '''
        cursor.execute(segment_features_table)
        print("segment_features table created")
    except:
        
        try: # delete existing table and recreate it
            cursor.execute("DROP TABLE IF EXISTS segment_features") 
            segment_features_table = '''
            CREATE TABLE segment_features(
                SEG_ID text PRIMARY KEY,
                SEG_Superset_Description text,
                SEG_Features text,
                FOREIGN KEY(SEG_ID) REFERENCES segments(SEG_ID)
            )
            '''
            cursor.execute(segment_features_table)
            print("segment_features table created")
        except: # any other exception
            conn.close()
            print("Unable to create segment_features table, closing connection.")
        conn.commit()

def generateSegmentFeatures(conn):
    cursor = conn.cursor()

    try: # create a temp table that contains all the categories' segments with the categories' extracted features
        cursor.execute("CREATE TEMP VIEW segment_category_features " +
                       "AS " + 
                       "SELECT " +
                           "categories.SEG_ID, " +
                           "category_features.CAT_Features " +
                       "FROM " +
                           "category_features " +
                            "INNER JOIN categories USING(CAT_ID); ")
        
        cursor.execute("SELECT SEG_ID FROM segments")  # execute a simple SQL select query
        segments = cursor.fetchall()
        

        i = 1

        # for each segment in the database extract features using yake module
        start2_time = time.time()
        for seg in segments:
            
            sys.stdout.write("\rSegment proccessed: %d" % i)
            i = i+1
            
            if(seg[0]== None):
                continue 
            
            # run sql command to get all extracted features of the categories under each segment grouped together -> the superset description for each segment
            cursor.execute("SELECT group_concat(CAT_Features) " + 
                           "FROM segment_category_features " +
                           "WHERE SEG_ID = '" +
                           str(seg[0]) +
                           "' GROUP BY SEG_ID")
            seg_superset_desc = cursor.fetchall()
           
            # runs yake on the superset of features of all categories under the given segment and returns a set of keywords
            k = yake_features(seg_superset_desc[0][0])
            
            # insert into the segment features table using SEG_ID of segment and the extracted features
            cursor.execute('''
                           INSERT INTO segment_features(SEG_ID, SEG_Superset_Description,SEG_Features)
                           VALUES (?,?,?)
                           ''',
                           tuple((str(seg[0]), str(seg_superset_desc[0][0]), k))
                           )
        
        # commit to database once all the segments are processed
        conn.commit()
        end2_time = time.time()
        print("--- Feature Extraction Time: " + str(end2_time-start2_time) + " seconds ---")
    except:
        conn.close()
        print("There was an issue with the segments")
        traceback.print_exc()
    
def main(dbName):

    # connect to sql server
    conn = getConnection(dbName)
    cursor = conn.cursor()
    
    # create SegmentFeatures table to store segment features
    createSegmentFeaturesTable(conn)

    # extract features for the segments
    generateSegmentFeatures(conn)
        
    #Close the connection
    conn.close()
    
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("database_name")
    args = parser.parse_args()

    main(args.database_name)