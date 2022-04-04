# -*- coding: utf-8 -*-
"""
Created on Tue Mar 22 16:02:41 2022

@author: crodr
"""

# import libraries
#import os
#from operator import index
import numpy as np
import pandas as pd
import sqlite3
import re
import nltk
import string
import traceback
from bs4 import BeautifulSoup
import time


def getConnection(dbName):
    conn = sqlite3.connect(dbName)
    return conn

"""
def removeHTML(description):
    
    partial = BeautifulSoup(description, 'lxml')
    for text in partial(['style', 'script']):
        text.decompose()
    return ' '.join(partial.stripped_strings)

def cleanHTML(df): #remove all html
    for d in df.itertuples():
        df['description'] = removeHTML(str(df['description']))
    #print(df['description'].head())
    return df   
"""
       
def otherHTML(data):
    
    for x in data.index:
        #txt = str(row[1])
        data.loc[x,'description']= BeautifulSoup(data.loc[x,'description'], 'lxml').text
        #print(txt)
    #print(df['description'].head())
    
    return data  


"""
def regexHTML(data):
    cleanD = re.compile('<.*?>')
    
    for x in data.index:
        clean_desc = re.sub(cleanD, '', data.loc[x,'description'])
        data.loc[x,'description'] = clean_desc
        #print(clean_desc)
    return data
"""

def lowerCase(df): #set all descriptions to lowercase
    #i = 0
    #for r in raw_descr:
        #df = df.append({'SKU': r[0], 'description': r[1].lower()},ignore_index = True)
        #df['description'] = r[1].lower()
        #i = i+1
        #print(i)
    df['description'] = df['description'].str.lower()   
    #print(df.head())
    return df


def PreProccessDescriptions(conn):
    cursor = conn.cursor()
    try:
        raw_descr =cursor.execute('SELECT SKU, CombinedDescription FROM products')
        #cursor.execute('SELECT SKU, CombinedDescription FROM products')
        #raw_descr = cursor.fetchall() #Stores all the returned rows
        
        
        df = pd.DataFrame(columns=['SKU', 'description'], dtype=object)
        
        sappend_time = time.time()
        i = 0
        for r in raw_descr:
            #if i == 10:
            #    break
            sku = r[0]
            desc = r[1]
            df = df.append({'SKU': sku, 'description': desc}, ignore_index = True)
            #i = i+1
            #print(r)
        eappend_time = time.time()
        print("---Populate time: %s seconds ---" % (eappend_time - sappend_time))
        
        scase_time = time.time()
        df = lowerCase(df) #Convert all text to lowercase
        ecase_time = time.time()
        print("---Case time: %s seconds ---" % (ecase_time - scase_time))
        
        shtml_time = time.time()
        #df = cleanHTML(df) #remove all HTML tags
        df = otherHTML(df) #remove all HTML tags
        #df = regexHTML(df) #remove all HTML tags
        #print(df['description'])
        ehtml_time = time.time()
        print("---HTML time: %s seconds ---" % (ehtml_time - shtml_time))
        
        test = "test'"
        ssql_time = time.time()
        for a in df.index:
            #print(df.loc[a,'description'])
            cursor.execute("UPDATE products SET CombinedDescription = ? WHERE SKU = ?", (str(df.loc[a,'description']), str(df.loc[a,'SKU']))) 
            #print(df.loc[a,'description'])
        conn.commit()
        esql_time = time.time()    
        print("---SQL time: %s seconds ---" % (esql_time - ssql_time))
    
    except:
        print('Error: Unable to preproccess.')
        traceback.print_exc()
        conn.close()
    

def main():
    # connect to sql server
    conn = getConnection("main.db")

    PreProccessDescriptions(conn)
    
    #Close database connection
    conn.close()

if __name__ == "__main__":
    main()



