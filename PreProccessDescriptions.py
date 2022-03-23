# -*- coding: utf-8 -*-
"""
Created on Tue Mar 22 16:02:41 2022

@author: crodr
"""

# import libraries
import os
from operator import index
import numpy as np
import pandas as pd
import sqlite3
import re
import nltk
import string
import traceback
from bs4 import BeautifulSoup
import time

# connect to sql server
databaseName = "test2.db"
conn = sqlite3.connect(databaseName)
cursor = conn.cursor()

def HTML(text):
    return BeautifulSoup(text, "lxml").text

def cleanHTML(df): #remove all html 
    df['description'] = df['description'].apply(HTML)
    #print(df.head())
    return df   

def lowerCase(df): #set all descriptions to lowercase
    #i = 0
    for r in raw_descr:
        #df = df.append({'SKU': r[0], 'description': r[1].lower()},ignore_index = True)
        df['description'] = r[1].lower()
        #i = i+1
        #print(i)
        
    #print(df.head())
    return df



try:
    raw_descr =cursor.execute('SELECT SKU, CombinedDescription FROM products')
    #raw_descr = cursor.fetchall() #Stores all the returned rows
    
    df = pd.DataFrame(columns=['SKU', 'description'])
    
    sappend_time = time.time()
    for r in raw_descr:
        df = df.append({'SKU': r[0], 'description': r[1]}, ignore_index = True)
    eappend_time = time.time()
    print("---Case time: %s seconds ---" % (eappend_time - sappend_time))
    
    """
    scase_time = time.time()
    df = lowerCase(df) #Convert all text to lowercase
    ecase_time = time.time()
    print("---Case time: %s seconds ---" % (ecase_time - scase_time))
    """
    shtml_time = time.time()
    df = cleanHTML(df) #remove all HTML tags
    ehtml_time = time.time()
    print("---HTML time: %s seconds ---" % (ehtml_time - shtml_time))

    ssql_time = time.time()
    for d in df:
        print(d)
        cursor.execute("UPDATE products SET CombinedDescription = '" + str(d[1]) + "' WHERE SKU = " + str(d[0]))
    conn.commit()
    esql_time = time.time()    
    print("---SQL time: %s seconds ---" % (esql_time - ssql_time))
    
except:
    print('Error: Unable to preproccess.')
    traceback.print_exc()
    conn.close()
    
# Close Connection
conn.close()