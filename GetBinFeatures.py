# -*- coding: utf-8 -*-
"""
Created on Sat Mar  5 17:23:54 2022

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

# 1) Combine all of the features of the products in each subclass (traverse breadth-first)
# Method: Join tables features adn products, walk through the subclasses table
# for each we run a query and griup concat and ebter the super features text 
# run rake on each subclass features text to make features for subclass

try: # create features table
    
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
    
    try: #delete existing table and remake it
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
    except:
        conn.close()
        print("Unable to create subclass_features table, closing connection.")


try: 
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
    
    r = Rake()
    i = 1
    #l = len(subclasses)
    for sub in subclasses:
        #print(str(sub[0]))
        #if (i/l * 100)%3 == 0:
        #    print(i/l * 100)
        print("Subclassed proccessed:" + str(i))
        i = i+1
        cursor.execute("SELECT group_concat(Features) " + 
                       "FROM subclass_product_features " +
                       "WHERE SubClass = '" +
                       str(sub[0]) +
                       "' GROUP BY SubClass")
        sub_superset_desc = cursor.fetchall()
        #print(sub_superset_desc[0][0])
        r.extract_keywords_from_text(sub_superset_desc[0][0])
        k = r.get_ranked_phrases()
        k = ', '.join(k)
        cursor.execute('''
                       INSERT INTO subclass_features(SUB_ID, SUB_Superset_Description, SUB_Features)
                       VALUES (?,?,?)
                       ''',
                       tuple((str(sub[0]), str(sub_superset_desc[0][0]), k))
                       )
        conn.commit()
except:
    conn.close()
    print("There was an issue with the subclasses")
    traceback.print_exc()

#Close the connection
conn.close()