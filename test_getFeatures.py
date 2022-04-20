import pytest
import pandas
import os
import sqlite3
import nltk

import GetFeatures
from GetConnection import getConnection

def test_checkTableExists_features():
    conn = getConnection("test2.db", False)
    GetFeatures.createFeaturesTable(conn)
    cursor = conn.cursor()
    stmt = "SELECT name FROM sqlite_master WHERE type='table' AND name='features'"
    cursor.execute(stmt)
    result = cursor.fetchone()
    print(result)
    assert result[0] == "features"


def test_featuresAlreadyExists(capsys):
    db = "test2.db"
    conn = getConnection(db, True)
    GetFeatures.createFeaturesTable(conn)
    expectedMsg = "Features table already exists, deleting and re-creating"
    captured = capsys.readouterr()
    print(captured)
    assert expectedMsg not in captured.out
    
    GetFeatures.createFeaturesTable(conn)
    captured = capsys.readouterr()
    assert expectedMsg in captured.out

def test_checkTableIsNotNull_features():
    GetFeatures.main("test2.db")
    conn = getConnection("test2.db", False)
    cursor = conn.cursor()

    stmt = "SELECT * from features"
    cursor.execute(stmt)
    result = cursor.fetchall()

    print(result)
