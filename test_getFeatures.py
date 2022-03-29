import pytest
import pandas
import os
import sqlite3
import nltk

import GetFeatures

def test_getConnection():
    GetFeatures.getConnection("test1")
    assert os.path.exists("test1")
    if os.path.exists("test1"):
        os.remove("test1")

def test_checkTableExists_features():
    conn = GetFeatures.getConnection("test2.db")
    GetFeatures.createFeaturesTable(conn)
    cursor = conn.cursor()
    stmt = "SELECT name FROM sqlite_master WHERE type='table' AND name='features'"
    cursor.execute(stmt)
    result = cursor.fetchone()
    assert result[0] == "features"
    