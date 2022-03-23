# import libraries
import os
from operator import index
import numpy as np
import pandas as pd
import sqlite3

# import data set
def importDataSet(data):
    dataset = pd.read_csv(data, index_col=False)
    print("Data set read!")
    return dataset

# connect to sql server
# conn = sqlite3.connect(":memory:")
def getConnection(dbName):
    if os.path.exists(dbName):
        os.remove(dbName)
    conn = sqlite3.connect(dbName)
    return conn

def main():
    dataset = importDataSet('Capstone_Dataset_1.csv')
    conn = getConnection('test2.db')
    cursor = conn.cursor()

    # create tables

    try: #Builds segments table
        segment_table = '''
        CREATE TABLE segments(
            SEG_ID text PRIMARY KEY,
            SEG_NAME text
        )
        '''
        cursor.execute(segment_table)
        print("Segment table created")
    except:
        
        try:#delete existing table and remake it
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
            conn.close()
            print("Unable to create segment table, closing connection.")

    try: #Builds categories table
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
        
        try:#delete existing table and remake it
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
            conn.close()
            print("Unable to create category table, closing connection.")
            
    try: #Builds classes table
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
        
        try:#delete existing table and remake it
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
            conn.close()
            print("Unable to create class table, closing connection.")

    try: #Builds subclasses table
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
        
        try:#delete existing table and remake it
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
        except:
            conn.close()
            print("Unable to create products table, closing connection.")

    for row in dataset.itertuples():
        cursor.execute('''
            INSERT OR IGNORE INTO segments (SEG_ID, SEG_NAME)
            VALUES (?, ?)
            ''',
            tuple(row)[11:13]
        )
        
        cursor.execute('''
            INSERT OR IGNORE INTO categories (CAT_ID, CAT_NAME, SEG_ID)
            VALUES (?, ?, ?)
            ''',
            tuple((row[13], row[14], row[11]))
        )
        
        cursor.execute('''
            INSERT OR IGNORE INTO classes (CLS_ID, CLS_NAME, CAT_ID)
            VALUES (?, ?, ?)
            ''',
            tuple((row[15],row[16],row[13]))
        )
        
        cursor.execute('''
            INSERT OR IGNORE INTO subclasses (SUB_ID, SUB_NAME, CLS_ID)
            VALUES (?, ?, ?)
            ''',
            tuple((row[17],row[18],row[15]))
        )
        
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

if __name__ == "__main__":
    main()