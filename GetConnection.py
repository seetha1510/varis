import os
import sys
import sqlite3

def getConnection(dbName, isDeleteIfExists=False):
    '''
    This function returns a Connection object that represents the specified database. From this object, a cursor can be created to execute SQL commands on a database.
    isDeleteIfExists flag is included to remove all contents of the database if set to True. It is defaulted to False, so that all changes are committed.
    '''
    
    # if flag is set to true and path exists to dbName, remove the contents of the database
    if isDeleteIfExists and os.path.exists(dbName):
        os.remove(dbName)
    # create Connection object which is linked to dbName and return it
    conn = sqlite3.connect(dbName)
    return conn