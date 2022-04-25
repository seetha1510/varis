import os
import sys
from operator import index
import numpy as np
import pandas as pd
import sqlite3
import re
import traceback

# connect to sql server

def getConnection(dbName=sys.argv[1]):
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


def main(dbName):
    conn = getConnection(dbName)

    combineDescriptions(conn)
    #Close database connection
    conn.close()

if __name__ == "__main__":
    main()