# -*- coding: utf-8 -*-
"""
Created on Thu Mar  3 16:36:49 2022

@author: Owner
"""
# import libraries
import os
from operator import index
import numpy as np
import pandas as pd
import sqlite3
import re
import traceback

# connect to sql server
# conn = sqlite3.connect(":memory:")

def getConnection(dbName):
    conn = sqlite3.connect(dbName)
    return conn

def combineDescriptions(conn):
    cursor = conn.cursor()
    try: #Combine the descriptions and store to dedicated column
        cursor.execute("UPDATE products SET CombinedDescription = COALESCE(ItemTitle, '') || ' ' || COALESCE(ItemDescription, '') || ' ' || COALESCE(ItemBulletPoint, '') || ' ' || COALESCE(ItemDescription_2, '') || ' ' || COALESCE(ItemFactTag, '') || ' ' || COALESCE(Manufacturer, '') || ' ' || COALESCE(SellUOM, '') || ' ' || COALESCE(ItemPrice, '') || ' ' || COALESCE(SegmentName, '') || ' ' || COALESCE(CategoryName, '') || ' ' || COALESCE(ClassName, '') || ' ' || COALESCE(SubClassName, '')")
        conn.commit()
    except:
        conn.close()
        print("Unable to build the combined descriptions column.")
        traceback.print_exc()

    """
    try: #strip out html
        cursor.execute("SELECT SKU, CombinedDescription FROM products")
        description = cursor.fetchall()
        
        #Need to clean out HTML tags from descriptions
        cleanD = re.compile('<.*?>')
        def HTMLStrip(desc):
            clean_desc = re.sub(cleanD, '', desc)
            return clean_desc
        
        #Clean out each description and then reinsert it to the table column
        i = 1
        #cursor.execute("SET QUOTED_IDENTIFIER OFF;")
        for d in description:
            clean_desc = HTMLStrip(str(d[1]))
            print("Row: " + str(i) + " : "+ clean_desc)
            i = i+1
            cursor.execute("UPDATE products SET CombinedDescription = '" + clean_desc + "' WHERE SKU = " + str(d[0]))
    except:
        conn.close()        
    """

def main(dbName):
    conn = getConnection(dbName)

    combineDescriptions(conn)
    #Close database connection
    conn.close()

if __name__ == "__main__":
    main()