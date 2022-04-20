import os
import sys
import sqlite3

def getConnection(dbName, isDeleteIfExists=False):
    if isDeleteIfExists and os.path.exists(dbName):
        os.remove(dbName)
    conn = sqlite3.connect(dbName)
    return conn