import pytest
import sqlite3
import pandas
import os
import GetBinFeatures
import extract_data

def test_getConnection():
    GetBinFeatures.getConnection("test1")
    assert os.path.exists("test1")
    if os.path.exists("test1"):
        os.remove("test1")

def test_checkTableExists_subclassFeatures():
    # check to see if subclass features table is created in "test2.db"
    conn = GetBinFeatures.getConnection("test2.db")
    GetBinFeatures.createSubclassFeaturesTable(conn)
    cursor = conn.cursor()
    stmt = "SELECT name FROM sqlite_master WHERE type='table' AND name='subclass_features'"
    cursor.execute(stmt)
    result = cursor.fetchone()
    # print(len(result))
    assert result[0] == "subclass_features"


# TODO: optimize code or create dummy data for shorter run time

def test_combineSubclassFeatures():
    # check to see if column with superset descriptions is created
    conn = GetBinFeatures.getConnection("test2.db")
    GetBinFeatures.createSubclassFeaturesTable(conn)
    #GetBinFeatures.combineSubclassFeatures(conn)
    cursor = conn.cursor()
    stmt = "PRAGMA table_info(subclass_features)"
    cursor.execute(stmt)
    result = cursor.fetchall()
    print("result:", result)
