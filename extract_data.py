# import libraries
import os
from operator import index
import numpy as np
import pandas as pd
import sqlite3

# import data set
dataset = pd.read_csv('Capstone_Dataset_1.csv', index_col=False)
print("Data set imported!")

# connect to sql server
# conn = sqlite3.connect(":memory:")
databaseName = "test2.db"
if os.path.exists(databaseName):
    os.remove(databaseName)
conn = sqlite3.connect("test2.db")
cursor = conn.cursor()

# create table
product_table = '''
CREATE TABLE products(
    SKU int PRIMARY KEY,
    ItemTitle text,
    ItemDescription text,
    ItemBulletPoint text,
    ItemDescription_2 text,
    Manufacturer text,
    MfrPartNum text,
    SellUOM text,
    ItemPrice float,
    ItemFactTag text,
    Segment text,
    SegmentName text,
    Category text,
    CategoryName text,
    Class text, 
    ClassName text,
    SubClass text,
    SubClassName text,
    CombinedDescription text
)
'''

cursor.execute(product_table)
for row in dataset.itertuples():
    cursor.execute('''
        INSERT INTO products (SKU, ItemTitle, ItemDescription, ItemBulletPoint, ItemDescription_2, Manufacturer, MfrPartNum, SellUOM, ItemPrice, ItemFactTag, Segment, SegmentName, Category, CategoryName, Class, ClassName, SubClass, SubClassName)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        tuple(row)[1:]
    )
conn.commit()

# handle missing data

# handle categorical data

# split data into training and testing sets

# feature scale

#Close connection
conn.close()