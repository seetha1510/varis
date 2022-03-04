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

# connect to sql server
# conn = sqlite3.connect(":memory:")
databaseName = "test2.db"
conn = sqlite3.connect(databaseName)
cursor = conn.cursor()

try:
    #Combine the descriptions and store to dedicated column
    cursor.execute("UPDATE products SET CombinedDescription = COALESCE(ItemTitle, '') || ' ' || COALESCE(ItemDescription, '') || ' ' || COALESCE(ItemBulletPoint, '') || ' ' || COALESCE(ItemDescription_2, '')")
    conn.commit()
except:
    conn.close()


#Close database connection
conn.close()

