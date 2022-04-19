# -*- coding: utf-8 -*-
"""
Created on Sat Mar  5 23:09:26 2022

@author: Owner
"""

# import libraries
import os
from operator import index
import numpy as np
import pandas as pd
import sqlite3
from rake_nltk import Rake
import nltk
import traceback
import time
import yake

def yake_features(text, max_words=80, duplicates=0.5, phrase_size=3):
    #ystart = time.time()
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
    #print(a)
    #yend = time.time()
    #print("--- Yake time: " + str(yend - ystart) + " seconds ---")
    return a

def main(dbName):

    # connect to sql server
    # conn = sqlite3.connect(":memory:")
    databaseName = dbName
    conn = sqlite3.connect(databaseName)
    cursor = conn.cursor()
    
    try: # create features table  
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
        
        try: #delete existing table and remake it
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
        except:
            conn.close()
            print("Unable to create segment_features table, closing connection.")
    
    
    try:# aggregate features from the categories and redo a keyword search on them to create segment features
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
        
        #r = Rake()
        i = 1
        #l = len(segments)
        start2_time = time.time()
        for seg in segments:
            #print(str(sub[0]))
            #if (i/l * 100)%3 == 0:
            #    print(i/l * 100)
            print("Segmented proccessed:" + str(i))
            i = i+1
            
            if(seg[0]== None):
                continue 
            
            cursor.execute("SELECT group_concat(CAT_Features) " + 
                           "FROM segment_category_features " +
                           "WHERE SEG_ID = '" +
                           str(seg[0]) +
                           "' GROUP BY SEG_ID")
            seg_superset_desc = cursor.fetchall()
            #print(seg_superset_desc[0][0])
            """
            r.extract_keywords_from_text(seg_superset_desc[0][0])
            k = r.get_ranked_phrases()
            k = ', '.join(k)
            """
            k = yake_features(seg_superset_desc[0][0])
            cursor.execute('''
                           INSERT INTO segment_features(SEG_ID, SEG_Superset_Description,SEG_Features)
                           VALUES (?,?,?)
                           ''',
                           tuple((str(seg[0]), str(seg_superset_desc[0][0]), k))
                           )
        conn.commit()
        end2_time = time.time()
        print("--- Feature Extraction Time: " + str(end2_time-start2_time) + " seconds ---")
    except:
        conn.close()
        print("There was an issue with the segments")
        traceback.print_exc()
        
    #Close the connection
    conn.close()