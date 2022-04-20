import os
import sys
from operator import index
import numpy as np
import pandas as pd
import sqlite3
import re
import traceback

from GetConnection import getConnection

def combineDescriptions(conn):
    cursor = conn.cursor()
    try: #Combine the descriptions and store to dedicated column
        cursor.execute("UPDATE products SET CombinedDescription = COALESCE(ItemTitle, '') || ' ' || COALESCE(ItemDescription, '') || ' ' || COALESCE(ItemBulletPoint, '') || ' ' || COALESCE(ItemDescription_2, '') || ' ' || COALESCE(ItemFactTag, '') || ' ' || COALESCE(Manufacturer, '') || ' ' || COALESCE(SellUOM, '') || ' ' || COALESCE(ItemPrice, '') || ' ' || COALESCE(SegmentName, '') || ' ' || COALESCE(CategoryName, '') || ' ' || COALESCE(ClassName, '') || ' ' || COALESCE(SubClassName, '')")
        conn.commit()
    except Exception as e:
        print("Unable to build the combined descriptions column.")
        raise e


def main(dbName):
    conn = getConnection(dbName, False)

    combineDescriptions(conn)
    #Close database connection
    conn.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("database_name")
    args = parser.parse_args()

    main(args.database_name)