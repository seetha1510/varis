# import libraries
import sqlite3

# connect to sql server
databaseName = "test2.db"
conn = sqlite3.connect(databaseName)
cursor = conn.cursor()

def read_query(conn, query):
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except:
        conn.close()
        print("Unable to create features table, closing connection.")

        
skus = []
features = []

query = "SELECT SKU from features"
skus = read_query(conn, query)

query = "SELECT features from features"
features = read_query(conn, query)

dic = dict(zip(skus, features))

#dictionary_items = dic.items()
#for item in dictionary_items:
    #print(item)
    

