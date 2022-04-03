# -*- coding: utf-8 -*-
"""
Created on Sat Mar  5 22:43:37 2022
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

# connect to sql server
# conn = sqlite3.connect(":memory:")
databaseName = "test2.db"
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
    
    r = Rake()
    i = 1
    #l = len(categories)
    for cat in categories:
        #print(str(sub[0]))
        #if (i/l * 100)%3 == 0:
        #    print(i/l * 100)
        print("Categoried proccessed:" + str(i))
        i = i+1
        cursor.execute("SELECT group_concat(CLS_Features) " + 
                       "FROM category_class_features " +
                       "WHERE CAT_ID = '" +
                       str(cat[0]) +
                       "' GROUP BY CAT_ID")
        cat_superset_desc = cursor.fetchall()
        #print(cat_superset_desc[0][0])
        r.extract_keywords_from_text(cat_superset_desc[0][0])
        k = r.get_ranked_phrases()
        k = ', '.join(k)
        cursor.execute('''
                       INSERT INTO category_features(CAT_ID, CAT_Superset_Description,CAT_Features)
                       VALUES (?,?,?)
                       ''',
                       tuple((str(cat[0]), str(cat_superset_desc[0][0]), k))
                       )
        conn.commit()
except:
    conn.close()
    print("There was an issue with the cats")
    traceback.print_exc()
    
#Close the connection
conn.close()