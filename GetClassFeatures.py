# -*- coding: utf-8 -*-
"""
Created on Sat Mar  5 22:04:00 2022

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

# connect to sql server
# conn = sqlite3.connect(":memory:")
databaseName = "UAT_2.db"
conn = sqlite3.connect(databaseName)
cursor = conn.cursor()

def yake_features(text, max_words=40, duplicates=0.5, phrase_size=3):
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
    
    #r = Rake()
    i = 1
    #l = len(classes)
    #we are calling the iteration variable cla because cls is apperently a reserved word
    start2_time = time.time()
    for cla in classes:
        #print(str(sub[0]))
        #if (i/l * 100)%3 == 0:
        #    print(i/l * 100)
        print("Classed proccessed:" + str(i))
        i = i+1
        
        if(cla[0]== None):
            continue 
        
        cursor.execute("SELECT group_concat(SUB_Features) " + 
                       "FROM class_subclass_features " +
                       "WHERE CLS_ID = '" +
                       str(cla[0]) +
                       "' GROUP BY CLS_ID")
        cls_superset_desc = cursor.fetchall()
        #print(cls_superset_desc[0][0])
        """
        r.extract_keywords_from_text(cls_superset_desc[0][0])
        k = r.get_ranked_phrases()
        k = ', '.join(k)
        """
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
    
#Close the connection
conn.close()