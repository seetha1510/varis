# -*- coding: utf-8 -*-
"""
Created on Thu Mar  3 17:26:01 2022

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
#nltk.download('stopwords')
#nltk.download('punkt')

# connect to sql server
# conn = sqlite3.connect(":memory:")
databaseName = "test2.db"
conn = sqlite3.connect(databaseName)
cursor = conn.cursor()

try:
    # create features table
    features_table = '''
    CREATE TABLE features(
        SKU int,
        Features text,
        FOREIGN KEY(SKU) REFERENCES products(SKU)
    )
    '''
    cursor.execute(features_table)
    #print("Ping1")
except:
    
    try: #delete existing table and remake it
        cursor.execute("DROP TABLE IF EXISTS features")
        
        features_table = '''
        CREATE TABLE features(
            SKU int,
            Features text,
            FOREIGN KEY(SKU) REFERENCES products(SKU)
        )
        '''
        cursor.execute(features_table)
        #print("Ping2")
    except:
        conn.close()
        #print("Ping3")


try:
    #Get all of the Combined Descriptions
    #rows = pd.read_sql_query("SELECT COUNT() FROM products ", conn)
    #descriptions = pd.read_sql_query("SELECT CombinedDescription FROM products ", conn)
    cursor.execute("SELECT SKU, CombinedDescription FROM products")  # execute a simple SQL select query
    descriptions = cursor.fetchall()
    
    #print("Ping4")
    #Initilize a Rake variable
    r = Rake()
    
    i = 0
    for d in descriptions:
        r.extract_keywords_from_text(d[1])
        k = r.get_ranked_phrases()
        k = ', '.join(k)
        a = int(d[0])
        cursor.execute('''
                       INSERT INTO features (SKU, Features)
                       VALUES (?,?)
                       ''',
                       tuple((a, k))
                       )
    conn.commit()
    
except:
    conn.close()
    #print("Ping5")

#Close the connection
conn.close()
#print("Ping6")