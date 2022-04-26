import sys
import numpy as np
import pandas as pd
import sqlite3
import re
import nltk
import string
import traceback
from bs4 import BeautifulSoup
import time

# Connects to SQLite database.
def getConnection(dbName=sys.argv[1]):
    conn = sqlite3.connect(dbName)
    return conn

# Strips out HTML tags from the string passed in, returns "cleaned" string.     
def otherHTML(data):
    
    for x in data.index:
        data.loc[x,'description']= BeautifulSoup(data.loc[x,'description'], 'lxml').text
    
    return data  

# Sets all alpha charcters in the given string to lowercase, returns modified string.
def lowerCase(df): #set all descriptions to lowercase

    df['description'] = df['description'].str.lower()   
    return df


def PreProccessDescriptions(conn):
    cursor = conn.cursor()
    try:
        # Grabs the products' combined description strings and stores them to memory for maniplulation.
        raw_descr =cursor.execute('SELECT SKU, CombinedDescription FROM products')    
        df = pd.DataFrame(columns=['SKU', 'description'], dtype=object)
        
        sappend_time = time.time()
        i = 0
        for r in raw_descr:
            sku = r[0]
            desc = r[1]
            df = df.append({'SKU': sku, 'description': desc}, ignore_index = True)      
        eappend_time = time.time()
        print("---Populate time: %s seconds ---" % (eappend_time - sappend_time)) 
        
        # Peforms lowercasing operation.
        scase_time = time.time()
        df = lowerCase(df) #Convert all text to lowercase
        ecase_time = time.time()
        print("---Case time: %s seconds ---" % (ecase_time - scase_time))
        
        # Performs HTML stripping operation.
        shtml_time = time.time()
        df = otherHTML(df) #remove all HTML tags
        ehtml_time = time.time()
        print("---HTML time: %s seconds ---" % (ehtml_time - shtml_time))
        
        # Stores the processed strings back into the database.
        ssql_time = time.time()
        for a in df.index:   
            cursor.execute("UPDATE products SET CombinedDescription = ? WHERE SKU = ?", (str(df.loc[a,'description']), str(df.loc[a,'SKU'])))    
        conn.commit()
        esql_time = time.time()    
        print("---SQL time: %s seconds ---" % (esql_time - ssql_time))
    
    except:
        print('Error: Unable to preproccess.')
        traceback.print_exc()
        conn.close()
    

def main(dbName):
    # connect to sql server
    conn = getConnection(dbName)

    PreProccessDescriptions(conn)
    
    #Close database connection
    conn.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("database_name")
    args = parser.parse_args()

    main(args.database_name)

    


