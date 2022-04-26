# import libraries
import os
import sys
from operator import index
import numpy as np
import pandas as pd
import sqlite3

from GetConnection import getConnection

# import data set
def importDataSet(data):
    '''
    This function returns a pandas DataFrame that is read in from a .csv file.
    '''

    # read data which is a csv file into a DataFrame
    dataset = pd.read_csv(data, index_col=False)
    return dataset

def createTables(conn):
    '''
    This function connects to a specified database and creates the necessary tables for the data to be read in. If a table already exists on that database, then it is deleted and re-created.
    '''

    # create cursor object to execute SQL statements on specified database
    cursor = conn.cursor()
    try: # builds segments table
        segment_table = '''
        CREATE TABLE segments(
            SEG_ID text PRIMARY KEY,
            SEG_NAME text
        )
        '''
        cursor.execute(segment_table)
        print("Segment table created")
    except:
        # delete existing table and remake it
        print("Segment table already exists, deleting and re-creating")
        try:
            cursor.execute("DROP TABLE IF EXISTS segments")
            segment_table = '''
            CREATE TABLE segments(
                SEG_ID text PRIMARY KEY,
                SEG_NAME text
            )
            '''
            cursor.execute(segment_table)
            print("Segment table created")
        except:
            # close connection if unable to create table
            conn.close()
            print("Unable to create segment table, closing connection.")

    try: # builds categories table
        category_table = '''
        CREATE TABLE categories(
            CAT_ID text PRIMARY KEY ,
            CAT_NAME text,
            SEG_ID text NOT NULL,
            FOREIGN KEY(SEG_ID) REFERENCES segments(SEG_ID)
        )
        '''
        cursor.execute(category_table)
        print("Category table created")
    except:
        # delete existing table and remake it
        print("Category table already exists, deleting and re-creating")
        try:
            cursor.execute("DROP TABLE IF EXISTS categories")
            category_table = '''
            CREATE TABLE categories(
                CAT_ID text PRIMARY KEY,
                CAT_NAME text,
                SEG_ID text NOT NULL,
                FOREIGN KEY(SEG_ID) REFERENCES segments(SEG_ID)
            )
            '''
            cursor.execute(category_table)
            print("Category table created")
        except:
            # close connection if unable to create table
            conn.close()
            print("Unable to create category table, closing connection.")
            
    try: # build class table
        class_table = '''
        CREATE TABLE classes(
            CLS_ID text PRIMARY KEY,
            CLS_NAME text,
            CAT_ID text NOT NULL,
            FOREIGN KEY(CAT_ID) REFERENCES categories(CAT_ID)
        )
        '''
        cursor.execute(class_table)
        print("Class table created")
    except:
        # delete existing table and remake it
        print("Class table already exists, deleting and re-creating")
        try:
            cursor.execute("DROP TABLE IF EXISTS classes")
            class_table = '''
            CREATE TABLE classes(
                CLS_ID text PRIMARY KEY,
                CLS_NAME text,
                CAT_ID text NOT NULL,
                FOREIGN KEY(CAT_ID) REFERENCES categories(CAT_ID)
            )
            '''
            cursor.execute(class_table)
            print("Class table created")
        except:
            # close connection if unable to create table
            conn.close()
            print("Unable to create class table, closing connection.")

    try: # build subclass table
        subclass_table = '''
        CREATE TABLE subclasses(
            SUB_ID text PRIMARY KEY,
            SUB_NAME text,
            CLS_ID text NOT NULL,
            FOREIGN KEY(CLS_ID) REFERENCES classes(CLS_ID)
        )
        '''
        cursor.execute(subclass_table)
        print("SubClass table created")
    except:
        # delete existing table and remake it
        print("Subclass table already exists, deleting and re-creating")
        try:
            cursor.execute("DROP TABLE IF EXISTS subclasses")
            subclass_table = '''
            CREATE TABLE subclasses(
                SUB_ID text PRIMARY KEY,
                SUB_NAME text,
                CLS_ID text NOT NULL,
                FOREIGN KEY(CLS_ID) REFERENCES classes(CLS_ID)
            )
            '''
            cursor.execute(subclass_table)
            print("SubClass table created")
        except:
            # close connection if unable to create table
            conn.close()
            print("Unable to create SubClass table, closing connection.")

    try: #Builds product table
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
            CombinedDescription text,
            FOREIGN KEY(Segment) REFERENCES segments(SEG_ID),
            FOREIGN KEY(Category) REFERENCES categories(CAT_ID),
            FOREIGN KEY(Class) REFERENCES classes(CLS_ID),
            FOREIGN KEY(SubClass) REFERENCES subclasses(SUB_ID)
        )
        '''
        cursor.execute(product_table)
        print("Products table created")
    except:
        print("Product table already exists, deleting and re-creating")
        try: #delete existing table and remake it
            cursor.execute("DROP TABLE IF EXISTS products")
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
                CombinedDescription text,
                FOREIGN KEY(Segment) REFERENCES segments(SEG_ID),
                FOREIGN KEY(Category) REFERENCES categories(CAT_ID),
                FOREIGN KEY(Class) REFERENCES classes(CLS_ID),
                FOREIGN KEY(SubClass) REFERENCES subclasses(SUB_ID)
            )
            '''
            cursor.execute(product_table)
            print("Products table created")
        except Exception as e:
            # close connection if unable to create table
            print("Unable to create products table, closing connection.")
            raise e
            conn.close()
        conn.commit()

def insertIntoTables(conn, dataset):
    '''
    This function goes through a pandas DataFrame and iterates through the rows to insert data into their respective tables.
    '''
    # create cursor object to execute SQL statements on specified database
    cursor = conn.cursor()

    # iterate through the rows of the dataframe
    for row in dataset.itertuples():
        # inserting rows[11:13] into segments table
        cursor.execute('''
            INSERT OR IGNORE INTO segments (SEG_ID, SEG_NAME)
            VALUES (?, ?)
            ''',
            tuple(row)[11:13]
        )
        # inserting rows[13], rows[14], and rows[11] into categories table
        cursor.execute('''
            INSERT OR IGNORE INTO categories (CAT_ID, CAT_NAME, SEG_ID)
            VALUES (?, ?, ?)
            ''',
            tuple((row[13], row[14], row[11]))
        )
        # inserting rows[15], rows[16], and rows[13] into classes table
        cursor.execute('''
            INSERT OR IGNORE INTO classes (CLS_ID, CLS_NAME, CAT_ID)
            VALUES (?, ?, ?)
            ''',
            tuple((row[15],row[16],row[13]))
        )
        # inserting rows[17], rows[18], and rows[15] into subclasses table
        cursor.execute('''
            INSERT OR IGNORE INTO subclasses (SUB_ID, SUB_NAME, CLS_ID)
            VALUES (?, ?, ?)
            ''',
            tuple((row[17],row[18],row[15]))
        )
        # inserting rows[1:] into products table
        cursor.execute('''
            INSERT INTO products (SKU, ItemTitle, ItemDescription, ItemBulletPoint, ItemDescription_2, Manufacturer, MfrPartNum, SellUOM, ItemPrice, ItemFactTag, Segment, SegmentName, Category, CategoryName, Class, ClassName, SubClass, SubClassName)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            tuple(row)[1:]
        )
    conn.commit()


def main(fileName=None, dbName=None):
    if fileName is None:
        fileName = sys.argv[1]
    if dbName is None:
        dbName = sys.argv[2]

    print("filename", fileName, "dbname", dbName)
    dataset = importDataSet(fileName)
    conn = getConnection(dbName)
    cursor = conn.cursor()
    createTables(conn)
    insertIntoTables(conn, dataset)

    # close connection
    conn.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("file_name")
    parser.add_argument("database_name")
    args = parser.parse_args()
    main(args.file_name, args.database_name)