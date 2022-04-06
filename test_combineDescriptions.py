import pytest
import sqlite3
import pandas
import os

import CombineDescriptions

def test_getConnection(name): #Checks the module can make a connection to the database
    conn = CombineDescriptions.getConnection(str(name))
    assert os.path.exists(str(name))
    conn.close()
    if os.path.exists(str(name)):
        os.remove(str(name))

def test_Empty_combineDescriptions(name):
    conn = CombineDescriptions.getConnection(name)
    CombineDescriptions.combineDescriptions(conn)
    conn.close()


def test_combineDescriptions(name):
    conn = CombineDescriptions.getConnection(name)
    CombineDescriptions.combineDescriptions(conn)
    cursor = conn.cursor()
    stmt = "SELECT EXISTS(SELECT CombinedDescription FROM products)"
    cursor.execute(stmt)
    res = cursor.fetchone()
    conn.close()
    assert res[0] == 1


name = "test4.db"
test_getConnection(name)
#test_Empty_combineDescriptions(name)
test_combineDescriptions(name)
    